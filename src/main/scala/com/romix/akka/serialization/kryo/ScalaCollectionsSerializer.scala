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

import scala.collection.Traversable
import scala.collection.Map
import scala.collection.Set
import scala.collection.SortedSet
import scala.collection.SortedMap
import scala.collection.generic.SortedSetFactory
import scala.collection.immutable
import scala.collection.immutable.TreeSet
import scala.collection.immutable.TreeMap
import scala.collection.mutable.Builder
import scala.collection.generic.GenericCompanion
import java.lang.reflect.Constructor
//import java.lang.reflect.TypeVariable
//import java.util.Collection

import com.esotericsoftware.kryo.Generics
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.GenericSerializer
import com.esotericsoftware.kryo.Serializer
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

/***
 * This module provides helper classes for serialization of Scala collections.
 * Currently it supports Maps, Sets and any Traversable collections.
 * 
 * @author eedrls
 *
 */

abstract class TraversableSerializer[T, C <: Traversable[T]](val kryo: Kryo, typ: Class[_], targetClass: Class[_]) 
	extends GenericSerializer[C](kryo, typ, targetClass) {
	
	var elementsCanBeNull = true
	var length:Int = 0
	var serializer: Serializer[_] = null
	var elementClass: Class[_]  = null
	
	/** @param elementsCanBeNull False if all elements are not null. This saves 1 byte per element if elementClass is set. True if it
	 *           is not known (default). */
	def setElementsCanBeNull (_elementsCanBeNull: Boolean) = {
		elementsCanBeNull = _elementsCanBeNull
	}

	/** Sets the number of objects in the collection. Saves 1-2 bytes. */
	def setLength (_length: Int) = {
		length = _length
	}

	/** @param elementClass The concrete class of each element. This saves 1-2 bytes per element. The serializer registered for the
	 *           specified class will be used. Set to null if the class is not known or varies per element (default). */
	def setElementClass (_elementClass: Class[_]) = {
		elementClass = _elementClass
		serializer = if(elementClass == null) null else kryo.getRegistration(elementClass).getSerializer()
	}

	/** @param elementClass The concrete class of each element. This saves 1-2 bytes per element. Set to null if the class is not
	 *           known or varies per element (default).
	 * @param serializer The serializer to use for each element. */
	def setElementClass (_elementClass: Class[_], _serializer: Serializer[_]) = {
		elementClass = _elementClass
		serializer = _serializer
	}
	
	override def read(kryo: Kryo, input: Input, typ: Class[C]): C  = {
		val len = if (length != 0) length else input.readInt(true)
		
		// kryo.reference(null)
		
		var coll: Any = initCollection(kryo, input, typ)
		
		// FIXME: Currently there is no easy way to get the reference ID of the object being read
		val ref = coll
		val refResolver = kryo.getReferenceResolver
		kryo.reference(ref)
		val refId = refResolver.nextReadId(typ) - 1
		
		if (len != 0) {
			if (serializer != null) {
				if (elementsCanBeNull) {
					0 until len foreach {idx => coll = update(coll, idx, readObjectOrNull(kryo, input, elementClass, serializer).asInstanceOf[T])}
				} else {
					0 until len foreach {idx => coll = update(coll, idx, readObject(kryo, input, elementClass, serializer).asInstanceOf[T])}
				}
			} else {
				0 until len foreach {idx => coll = update(coll, idx, readClassAndObject(kryo, input).asInstanceOf[T])}
			}
		} 
		
		val c = finishCollection(coll, typ)
		refResolver.addReadObject(refId, c)
//		kryo.reference(c)
		c
	}

	def update(coll:Any, idx:Int, elem:T):Any = coll
	
	override def write (kryo : Kryo, output: Output, obj: C) = {
		val collection: Traversable[_] = obj
		val len = if (length != 0) length else {
			val size = collection.size
			output.writeInt(size, true)
			size
		}
		
		writeBeforeElements(kryo, output, obj)
			
		if (len != 0) { 
			if (serializer != null) {
				if (elementsCanBeNull) {
					collection foreach { e:Any => writeObjectOrNull(kryo, output, e, serializer) }
				} else {
					collection foreach { e:Any => writeObject(kryo, output, e, serializer) }
				}
			} else {
				collection foreach { e:Any => writeClassAndObject(kryo, output, e, serializer) }
			}
		}
	}
	
	// Can be used to write something that is required for constructing an (empty) instance of
	// a given type, e.g. Ordering as required by SortedMap
	def writeBeforeElements(kryo : Kryo, output: Output, obj: C) = {}

	// Create an empty collection or builder
	def initCollection(kryo: Kryo, input: Input, typ: Class[C]):Any 
//	= {
//		val inst = kryo.newInstance(typ)
//		// Create a builder
//		// inst.asInstanceOf[Traversable[Any]].genericBuilder[Any]
//		// Create an empty collection                                                   
//		inst.asInstanceOf[Traversable[Any]].companion.empty.asInstanceOf[C]		
//	}
	
	def finishCollection(coll: Any, typ: Class[C]):C 
	
	// Methods for reading and writing single elements of collections
	def writeObject(kryo: Kryo, output:Output, e:Any, serializer: Serializer[_]) = kryo.writeObject(output, e, serializer)
	
	def writeObjectOrNull(kryo: Kryo, output:Output, e:Any, serializer: Serializer[_]) = kryo.writeObjectOrNull(output, e, serializer)
	
	def writeClassAndObject(kryo: Kryo, output:Output, e:Any, serializer: Serializer[_]) = kryo.writeClassAndObject(output, e)
	

	def readObject(kryo: Kryo, input:Input, typ: Class[_], serializer: Serializer[_]) = kryo.readObject(input, typ, serializer)
	
	def readObjectOrNull(kryo: Kryo, input:Input, typ: Class[_], serializer: Serializer[_]) = kryo.readObjectOrNull(input, typ, serializer)
	
	def readClassAndObject(kryo: Kryo, input:Input) = kryo.readClassAndObject(input)
}

class ScalaCollectionSerializer (kryo: Kryo, typ: Class[_]) 
	extends TraversableSerializer[Any, Traversable[Any]](kryo, typ, classOf[Traversable[_]]) {
	
	override def setGenerics(kryo: Kryo, scope:Generics) = {
		val elemClazz = scope.getConcreteClass("A")
		if(elemClazz != null && kryo.isFinal(elemClazz))
			setElementClass(elemClazz, kryo.getSerializer(elemClazz))
	}
	

	override def initCollection(kryo: Kryo, input: Input, typ: Class[Traversable[Any]]):Any = {
		val inst = kryo.newInstance(typ)
		// Create a builder
		inst.asInstanceOf[Traversable[Any]].genericBuilder[Any]
	}
	
	override def update(coll:Any, idx:Int, elem:Any):Any = (coll.asInstanceOf[Builder[Any, _]] += elem)
	
	def finishCollection(coll: Any, typ: Class[Traversable[Any]]):Traversable[Any] = (coll.asInstanceOf[Builder[AnyRef, Traversable[Any]]]).result
}

//private[this] class  MapTupleSerializer(val kryo: Kryo, typ: Class[_]) extends GenericSerializer[C](kryo, typ, classOf[Tuple2[_,_]]) {
//	
//	var keySerializer: Serializer[_] = null
//	var valueSerializer: Serializer[_] = null
//	var keyClass: Class[_]  = null
//	var valueClass: Class[_]  = null
//	
//	override def read(kryo: Kryo, input: Input, typ: Class[Tuple2[_,_]]): Tuple2[_,_]  = {
//		if (serializer != null) {
//			if (elementsCanBeNull) {
//				collection.foreach {e:Any => kryo.writeObjectOrNull(output, e, serializer) }
//			} else {
//				collection.foreach {e:Any => kryo.writeObject(output, e, serializer) }
//			}
//		} else {
//			collection.foreach {e:Any => kryo.writeClassAndObject(output, e) }
//		}		
//	}
//	
//	override def write (kryo : Kryo, output: Output, obj: Tuple2[_,_]) = {
//		
//	}
//}

class ScalaMapSerializer (kryo: Kryo, typ: Class[_]) 
	extends TraversableSerializer[Any, Traversable[Any]](kryo, typ, classOf[scala.collection.Map[_,_]])  {
	var keySerializer: Serializer[_] = null
	var valueSerializer: Serializer[_] = null
	var keyClass: Class[_]  = null
	var valueClass: Class[_]  = null
	var class2constuctor = immutable.Map[Class[_], Constructor[_]]()
	val ref = new Object()
	val isSorted: Boolean = classOf[SortedMap[_,_]].isAssignableFrom(typ)
	var companion: GenericCompanion[Traversable] = null 

	// FIXME: Since elements are tuples, a reference id is written for each tuple.
    // If we write key and value on their own, without the parent tuple object, we don't waste space for it.
    // BTW, there can be no two equal tuples inside the same map, because it would mean that the same key
    // is mapped to the same value multiple times
	locally {
//		val tupleSerializer = new MapTupleSerializer(kryo, typ);
//		setElementClass(classOf[Tuple2[_,_]], tupleSerializer)
		setElementClass(classOf[Tuple2[_,_]])
		if(isSorted) {
			try {
					val constructor = 
						class2constuctor.get(typ) getOrElse 
						{  
						val constr = typ.getDeclaredConstructor(classOf[scala.math.Ordering[_]])
						class2constuctor += typ->constr
						constr
						} 
				} 
			} 
	}


	override def setGenerics(kryo: Kryo, scope: Generics) = {
		
		val keyClazz = scope.getConcreteClass("A");
		val valueClazz = scope.getConcreteClass("B");
		val tupleGenerics = new Array[Class[_]](2)
		
		if(keyClazz != null && kryo.isFinal(keyClazz)) {
			setKeyClass(keyClazz, kryo.getSerializer(keyClazz))
			tupleGenerics(0) = keyClazz
		}
		
		if(valueClazz != null && kryo.isFinal(valueClazz)) {
			setValueClass(valueClazz, kryo.getSerializer(valueClazz))
			tupleGenerics(1) = valueClazz
		}
		
		if(keyClazz != null || valueClazz != null)
			serializer.setGenerics(kryo, tupleGenerics)
			
	}
	

	/** @param keyClass The concrete class of each key. This saves 1-2 bytes per key. The serializer registered for the
	 *           specified class will be used. Set to null if the class is not known or varies per element (default). */
	def setKeyClass (_keyClass: Class[_]) = {
		keyClass = _keyClass
		keySerializer = if(keyClass == null) null else kryo.getRegistration(keyClass).getSerializer()
		
	}

	/** @param valueClass The concrete class of each key. This saves 1-2 bytes per key. The serializer registered for the
	 *           specified class will be used. Set to null if the class is not known or varies per element (default). */
	def setValueClass (_valueClass: Class[_]) = {
		valueClass = _valueClass
		valueSerializer = if(valueClass == null) null else kryo.getRegistration(valueClass).getSerializer()
	}
	

	/** @param keyClass The concrete class of each key. This saves 1-2 bytes per key. Set to null if the class is not
	 *           known or varies per element (default).
	 * @param serializer The serializer to use for each key. */
	def setKeyClass (_keyClass: Class[_], _serializer: Serializer[_]) = {
		keyClass = _keyClass
		keySerializer = _serializer
	}

	/** @param keyClass The concrete class of each key. This saves 1-2 bytes per key. Set to null if the class is not
	 *           known or varies per element (default).
	 * @param serializer The serializer to use for each key. */
	def setValueClass (_valueClass: Class[_], _serializer: Serializer[_]) = {
		valueClass = _valueClass
		valueSerializer = _serializer
	}
	

	override def initCollection(kryo: Kryo, input: Input, typ: Class[Traversable[Any]]):Any = {
		val coll: Map[Any, Any] = 
			if(isSorted) {
				// Read ordering and set it for this collection 
				implicit val mapOrdering = kryo.readClassAndObject(input).asInstanceOf[scala.math.Ordering[Any]]
				try {
				   val constructor = class2constuctor.get(typ).get
				   constructor.newInstance(mapOrdering).asInstanceOf[scala.collection.Map[Any,Any]].empty 
				} catch { case _:Throwable => companion.empty.asInstanceOf[scala.collection.Map[Any,Any]] }
	//			try typ.getDeclaredConstructor(classOf[scala.math.Ordering[_]]).newInstance(mapOrdering).asInstanceOf[Map[Any,Any]].empty 
	//			catch { case _ => kryo.newInstance(typ).asInstanceOf[Map[Any,Any]].empty }
			} else {
				val map = kryo.newInstance(typ).asInstanceOf[Map[Any,Any]].empty
				// kryo.reference(map)
				map
			}
		coll
	}
	
	override def writeBeforeElements(kryo :Kryo, output: Output, obj: Traversable[Any]) = {
	    if(classOf[SortedMap[_,_]].isAssignableFrom(obj.getClass())) {
			val ordering = obj.asInstanceOf[SortedMap[_,_]].ordering
			kryo.writeClassAndObject(output, ordering)
		}
	}
	
	override def update(coll:Any, idx:Int, elem:Any):Any = {
		var map = coll.asInstanceOf[scala.collection.Map[Any, Any]]
		map += elem.asInstanceOf[Tuple2[Any,Any]]
		map
	}
	
	override def finishCollection(coll: Any, typ: Class[Traversable[Any]]):Traversable[Any] = coll.asInstanceOf[Traversable[Any]]
	                                                                                                            
	override def writeObject(kryo: Kryo, output:Output, e:Any, serializer: Serializer[_]) = {
		e match {
			case (k,v) => {
				if(keySerializer != null) 
					kryo.writeObject(output, k, keySerializer)
				else	
					kryo.writeClassAndObject(output, k)
					
				if(valueSerializer != null) 
					kryo.writeObject(output, v, valueSerializer)
				else
					kryo.writeClassAndObject(output, v)
			}
		}
	}
	override def writeObjectOrNull(kryo: Kryo, output:Output, e:Any, serializer: Serializer[_]) = {
		e match {
		case (k,v) => {
			if(keySerializer != null) 
				kryo.writeObjectOrNull(output, k, keySerializer)
			else	
				kryo.writeClassAndObject(output, k)
				
			if(valueSerializer != null) 
				kryo.writeObjectOrNull(output, v, valueSerializer)
			else
				kryo.writeClassAndObject(output, v)
		}
	  }		
	}
	
	override def readObject(kryo: Kryo, input:Input, typ: Class[_], serializer: Serializer[_]) = {
		val k = if(keySerializer == null) kryo.readClassAndObject(input) else kryo.readObject(input, keyClass, keySerializer) 
		val v = if(valueSerializer == null) kryo.readClassAndObject(input) else kryo.readObject(input, valueClass, valueSerializer)
		(k,v)
	}
	
	override def readObjectOrNull(kryo: Kryo, input:Input, typ: Class[_], serializer: Serializer[_]) = {
		val k = if(keySerializer == null) kryo.readClassAndObject(input) else kryo.readObjectOrNull(input, keyClass, keySerializer) 
		val v = if(valueSerializer == null) kryo.readClassAndObject(input) else kryo.readObjectOrNull(input, valueClass, valueSerializer)
		(k,v)		
	}
}

class ScalaSetSerializer (kryo: Kryo, typ: Class[_]) 
	extends TraversableSerializer[Any, Traversable[Any]](kryo, typ, classOf[Set[_]]) {
	var class2constuctor = immutable.Map[Class[_], Constructor[_]]()
	val ref = new Object()
	val isSorted: Boolean = classOf[SortedSet[_]].isAssignableFrom(typ)
	var companion: GenericCompanion[Traversable] = null

	locally {
			if(isSorted) {
				try {
 					val constructor = 
 						class2constuctor.get(typ) getOrElse 
 						{  
 						val constr = typ.getDeclaredConstructor(classOf[scala.math.Ordering[_]])
 						class2constuctor += typ->constr
 						constr
 						} 
   				} 
   			} else 
   				companion =  typ.asInstanceOf[Traversable[Any]].companion
	}
	
	override def setGenerics(kryo: Kryo, scope:Generics) = {
		val elemClazz = scope.getConcreteClass("A")
		if(elemClazz != null && kryo.isFinal(elemClazz))
			setElementClass(elemClazz, kryo.getSerializer(elemClazz))
	}
		
	override def initCollection(kryo: Kryo, input: Input, typ: Class[Traversable[Any]]):Any = {
   		val coll: Set[Any] = 
       			if(isSorted) {
       				// Read ordering and set it for this collection 
       				//kryo.reference(ref)
       				implicit val setOrdering = kryo.readClassAndObject(input).asInstanceOf[scala.math.Ordering[Any]]
     				try {
     					val constructor = class2constuctor.get(typ).get 
     					constructor.newInstance(setOrdering).asInstanceOf[Set[Any]].empty 
       				} catch { 
       					case _:Throwable => kryo.newInstance(typ).asInstanceOf[Set[Any]].empty 
       				}
       			} else {
       				val set = kryo.newInstance(typ).asInstanceOf[Set[Any]].empty
       				//kryo.reference(set)
       				set
       			}
		coll
	}

	override def writeBeforeElements(kryo :Kryo, output: Output, obj: Traversable[Any]) = {
		if(classOf[SortedSet[_]].isAssignableFrom(obj.getClass())) {
			val ordering = obj.asInstanceOf[SortedSet[_]].ordering
			kryo.writeClassAndObject(output, ordering)
		}		
	}
	
	override def update(coll:Any, idx:Int, elem:Any):Any = {
		var set = coll.asInstanceOf[Set[Any]]
		set += elem
		set
	}
	
	override def finishCollection(coll: Any, typ: Class[Traversable[Any]]):Traversable[Any] = coll.asInstanceOf[Traversable[Any]]
}
