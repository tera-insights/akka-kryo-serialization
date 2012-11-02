/*******************************************************************************
 * Copyright 2012 Roman Levenstein
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.romix.akka.serialization.kryo

import scala.collection.Map
import scala.collection.immutable
import java.lang.reflect.Constructor
import java.lang.reflect.ParameterizedType
import java.lang.reflect.TypeVariable
import java.lang.reflect.Type
import java.util.Arrays;
import java.util.Collection;

import com.esotericsoftware.kryo.Generics
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.minlog.Log._

/***
 * This module provides helper classes for serialization of scala.Product-based classes.
 * This includes all Tuple classes.
 * 
 * @author Roman Levenstein
 *
 */

class ScalaProductSerializer ( val kryo: Kryo, val typ: Class[_]) extends Serializer[Product] {
	var elementsCanBeNull = true
	var serializer: Serializer[_] = null
	var elementClass: Class[_]  = null
	var length:Int = 0
	val constructor = typ.getDeclaredConstructors()(0)
	// Serializers for each element of a product. Null if serializer is not known 
	var serializers: Array[Serializer[_]] = null
	// Classes of each element of a product. Null if class is not known 
	var elementClasses: Array[Class[_]] = null
	var lastGenerics: Array[Class[_]] = null
    	
	implicit def bool2int(b:Boolean) = if (b) 1 else 0

	/***
	 * Find out which superclas or interface implements the scala.ProductXXX 
	 */
	private[this] def findProductInterface(t: Class[_]): Option[ParameterizedType] = {
			var curTyp = t
			while (curTyp != null) {			
				val genericInterfaces = curTyp.getGenericInterfaces()						
				genericInterfaces foreach { genInterface =>
					if(genInterface.isInstanceOf[ParameterizedType]) {	 
						val parameterizedType = genInterface.asInstanceOf[ParameterizedType]
						if (parameterizedType.getRawType.asInstanceOf[Class[_]].getName.startsWith("scala.Product")) {
							return Option(parameterizedType)
						}
					}
					// TODO: Support non-parametrized classes derived from Product, e.g. case classes?
				}
				val oldCurTyp:Class[_] = curTyp
				curTyp = oldCurTyp.getSuperclass
			}
			
			Option(null)
	}
	
	// TODO: If actual type parameters are known in advance, make use of it
	locally {
		if(DEBUG) debug("Init Product serializer for " + typ.getName)
		// Find superclass or interface which belongs to scala.ProductXXX
		val productInterface = findProductInterface(typ)
		
		val scope = productInterface match {
			case Some(pt) => Generics.buildGenericsScope(typ, pt.getRawType.asInstanceOf[Class[_]], null)
			case _       => null
		}
		
		productInterface match {
			case Some(pt) =>  {
				val productTypeName = pt.getRawType.asInstanceOf[Class[_]].getName
				val suffix = productTypeName.substring("scala.Product".length)
				try {
					val arity = Integer.parseInt(suffix)
					if(DEBUG) debug("Arity:" + suffix)
					setLength(arity) 
				} catch {
					case e:Throwable => {}
				}
			}
			case _ => 
		}
		
		if(scope != null) {
			if(scope.size == productInterface.get.getActualTypeArguments.length)
				if(DEBUG) debug("All type arguments are known in advance")
				
		    // Init serializers
			if(length > 0) {
				serializers = new Array[Serializer[_]](length)
				elementClasses = new Array[Class[_]](length)
				val typeArgs = productInterface.get.getActualTypeArguments
				
				0 until typeArgs.length foreach { 
					i => { 
						serializers(i) = typeArgs(i) match {
							case clazz: Class[_] =>  {
								elementClasses(i) = clazz
								kryo.getSerializer(clazz)
							}
							case tvar: TypeVariable[_] if scope.getConcreteClass (tvar.getName) != null => {
								val clazz = scope.getConcreteClass(tvar.getName)
								elementClasses(i) = clazz
								kryo.getSerializer(clazz)
							}
							case _ => null
						}
						
						if(DEBUG) debug("Serializer[" + i + "] is " + serializers(i))
					}
				}
				
			}
		}		
	}
	
	override def setGenerics(kryo: Kryo, generics: Array[Class[_]]) = {
		
		if(generics != null && !java.util.Arrays.equals(lastGenerics.asInstanceOf[Array[Object]], generics.asInstanceOf[Array[Object]])) {
			lastGenerics = generics
			
			if(elementClasses == null || elementClasses.length < generics.length)
				elementClasses = new Array[Class[_]](generics.length)
			if(serializers == null || serializers.length < generics.length)
				serializers = new Array[Serializer[_]](generics.length)
			(0 until generics.length) foreach { i => if(generics(i) != null && kryo.isFinal(generics(i))) {
					elementClasses(i) = generics(i)
					serializers(i) = kryo.getSerializer(generics(i))
				} else {
					elementClasses(i) = null
					serializers(i) = null					
				}
			}
		}
	}

	/** @param elementsCanBeNull False if all elements are not null. This saves 1 byte per element if elementClass is set. True if it
	 *           is not known (default). */
	def setElementsCanBeNull (_elementsCanBeNull: Boolean) = {
		elementsCanBeNull = _elementsCanBeNull
	}

	/** Sets the number of objects in the collection. Saves 1-2 bytes. */
	def setLength (_length: Int) = {
		length = _length
	}

	//override 
	def read1(kryo: Kryo, input: Input, typ: Class[Product]): Product  = {
		// FIXME: Works properly only if write was called before read
		val len = if (length != 0) length else input.readInt(true)
		val ref = new Object
		kryo.reference(ref)
		
		val elems: Array[Any] = new Array(len)
				
		
		if (len != 0) {
			if (serializer != null) {
				if (elementsCanBeNull) {
					0 until len foreach {i => elems(i) = kryo.readObjectOrNull(input, elementClass, serializer) }
				} else {
					0 until len foreach {i => elems(i) = kryo.readObject(input, elementClass, serializer) }
				}
			} else {
				0 until len foreach {i => elems(i) = kryo.readClassAndObject(input) }
			}
		} 

		constructor.newInstance(elems.asInstanceOf[Array[Object]]:_*).asInstanceOf[Product] 
	}
	
	override def read(kryo: Kryo, input: Input, typ: Class[Product]): Product  = {
		// FIXME: Works properly only if write was called before read
		val len = if (length != 0) length else input.readInt(true)
		val ref = new Object
		kryo.reference(ref)
		
		val elems: Array[Any] = new Array(len)
				
		
		if (len != 0) {
			if (serializers != null) {
				if (elementsCanBeNull) {
					0 until len foreach {i =>
						serializers(i) match { 
							case serializer: Serializer[_] => elems(i) = kryo.readObjectOrNull(input, elementClasses(i), serializer)
							case _ => elems(i) = kryo.readClassAndObject(input)
						}
					}
				} else {
					0 until len foreach {i => 
						serializers(i) match { 
							case serializer: Serializer[_] => elems(i) = kryo.readObject(input, elementClasses(i), serializer)
							case _ => elems(i) = kryo.readClassAndObject(input)
						}
					}
				}
			} else {
				0 until len foreach {i => elems(i) = kryo.readClassAndObject(input) }
			}
		} 

		constructor.newInstance(elems.asInstanceOf[Array[Object]]:_*).asInstanceOf[Product] 
	}

	override def write (kryo : Kryo, output: Output, obj: Product) = {
		val product: Product = obj
		val len = if (length != 0) length else {
			val size = product.productArity
			// output.writeInt(size, true)
			// Length is always the same for Products
			setLength(size)
			size
		}
	
		if (len != 0) {
			if (serializers != null) {
				if (elementsCanBeNull) {
					var i = 0
					product.productIterator.foreach {
						element => 
						serializers(i) match {
							case serializer: Serializer[_] => kryo.writeObjectOrNull(output, element, serializer)
							case _ => kryo.writeClassAndObject(output, element)
						}
						i = i + 1
					}
				} else {
					var i = 0
					product.productIterator.foreach {
						element => 
						serializers(i) match {
							case serializer: Serializer[_] => kryo.writeObject(output, element, serializer)
							case _ => kryo.writeClassAndObject(output, element)
						}
						i = i + 1
					}
				}
			} else {
				product.productIterator.foreach {element => kryo.writeClassAndObject(output, element) }
			}
		}
	}
}
