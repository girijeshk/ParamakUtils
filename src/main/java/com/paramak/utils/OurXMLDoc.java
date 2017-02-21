/**
 *    Copyright 2016 Girijesh Kaushik (girijeshk@gmail.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.paramak.utils;

import java.io.File;
import java.io.InvalidClassException;
import java.io.PrintWriter;
import java.util.*;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.stack.TLongStack;
import gnu.trove.stack.array.TLongArrayStack;

public class OurXMLDoc { // TODO: eventually get rid of using Trove collection if they are not big perf booster
	private final ObjectStore _objectStore = new ObjectStore();
	
	private static Class<?>[] ObjectClasses = new Class<?>[]{Node.class, Attribute.class, Element.class, TextNode.class}; // node and its derived classes
	
	public void compact(){
        _objectStore.compact();
	}
	
	public void loadFile(String filePath){
	      try {	
	          File inputFile = new File(filePath);
	          SAXParserFactory factory = SAXParserFactory.newInstance();
	          SAXParser saxParser = factory.newSAXParser();
	          UserHandler userhandler = new UserHandler();
	          _rootElementPtr = 0;
	          saxParser.parse(inputFile, userhandler);
	          
	          //compact();
	       } catch (Exception e) {
	          e.printStackTrace();
	       }		// use 
	}
	
	public void print(PrintWriter out) throws InvalidClassException{
		Element ele = new Element();
		Node node = new Node();
		Attribute attrib = new Attribute();
		TextNode tn = new TextNode();
		int level = -1;
		printElement(out, _rootElementPtr, level, node, ele, attrib, tn);
	}
	
	private void printElement(PrintWriter writer, long elementPtr, int level, Node node, Element ele, Attribute attrib, TextNode tn) throws InvalidClassException {
		++level;

		ele.attachInstance(elementPtr);
		String tagName = ele.getTagName();
		for(int i=0; i<level;++i) 
			writer.print("\t");
		writer.print("<");
		writer.print(tagName);
		
		// write attribute
		long attribPtr = ele.getAttribPtr();
		while(attribPtr > 0){
			attrib.attachInstance(attribPtr);
			writer.print(" ");
			writer.print(attrib.getName());
			writer.print("=\"");
			writer.print(attrib.getValue());
			writer.print("\"");
			attribPtr = attrib.getNextNodeHandle();
		}
		
		writer.print(">");
		writer.println();

		// write child elements
		long childPtr = ele.getChildPtr();
		while(childPtr > 0){
			node.attachInstance(childPtr);
			long nextChildPtr = node.getNextNodeHandle();
			if(node.getClassForInstance() == Element.class){
				printElement(writer, childPtr, level, node, ele, attrib, tn);
			}else{
				for(int i=0; i<level;++i) 
					writer.print("\t");
				writer.print("<![CDATA[");
				tn.attachInstance(childPtr);
				writer.print(tn.getText());
				writer.print("]]>");
				writer.println();
			}
			childPtr = nextChildPtr;
		}
		
		for(int i=0; i<level;++i) 
			writer.print("\t");
		writer.print("</");
		writer.print(tagName);
		writer.print(">");
		writer.println();
	}
	public String getInfo(){
		StringBuilder bldr = new StringBuilder();
		bldr.append(String.format("String ref:%d\n", StringRefs.size()));
		_objectStore.addDebugInfo(bldr);


		return bldr.toString();
	}
	class UserHandler extends DefaultHandler{
		private TLongStack _elementStack = new TLongArrayStack();
		private TLongStack _containerStack = new TLongArrayStack();
		private Element _eleWrapper1 = new Element();
		private TextNode _tnWrapper = new TextNode();
		private long _currentElementPtr = -1;
		private long _currentContainerPtr = -1;
		
		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			try{
				Element currentElement = _eleWrapper1;
				currentElement.attachInstance(_currentElementPtr);
				if(!currentElement.isValidEndTag(qName, null)){
					throw new SAXException(String.format("Unmatched tag qName()Start=%s qName()End=%s .", currentElement.getTagName(), qName));
				}
				if(_elementStack.size() > 0){
					_currentElementPtr = _elementStack.pop();
					_currentContainerPtr = _containerStack.pop();
					appendNode(currentElement.getPtr());
				}else {
					_rootElementPtr = _currentElementPtr;
				}
			}catch(Exception ex){
				ex.printStackTrace();
				throw new SAXException(ex);
			}
		}
		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {
			Element newElement = _eleWrapper1;
			newElement.newInstance();
			newElement.setTagName(qName);
			newElement.setAttribPtr(writeAttributes(attributes));
			
			if(_currentElementPtr > 0){
				_elementStack.push(_currentElementPtr);
				_containerStack.push(_currentContainerPtr);
			}
			
			_currentElementPtr = _currentContainerPtr = newElement.getPtr();
		}
		
		private Attribute _tempAttrib = new Attribute();
		private long writeAttributes(Attributes attributes) {
			long attribHandle = 0;

			for(int index=attributes.getLength()-1; index >= 0; --index){
				Attribute curr = _tempAttrib;
				curr.newInstance();
				curr.setName(attributes.getQName(index));
				curr.setValue(attributes.getValue(index));
				curr.setNextNodeHandle(attribHandle);
				attribHandle = curr.getPtr();
			}

			return attribHandle;
		}
		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			try {
				// skip start whitespaces
				while(length > 0){
					char cur = ch[start];
					if(Character.isWhitespace(cur)){
						++start;
						--length;
					}else{
						break;
					}
				}
				// skip end whitespaces
				while(length > 0){
					char cur = ch[start+length-1];
					if(Character.isWhitespace(cur)){
						--length;
					}else{
						break;
					}
				}
				
				if(length > 0){
					_tnWrapper.newInstance();
					_tnWrapper.setText(new String(ch, start, length));
					appendNode(_tnWrapper.getPtr());
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new SAXException(e);
			}
		}
		Node _nodePtr = new Node();
		Element _ele2 = new Element(); 
		private void appendNode(long childNodePtr) throws InvalidClassException {
			if(_currentContainerPtr == _currentElementPtr){ // set child
				_ele2.attachInstance(_currentElementPtr);
				_ele2.setChildPtr(childNodePtr);
			}else{ // set sibling
				_nodePtr.attachInstance(_currentContainerPtr);
				_nodePtr.setNextNodeHandle(childNodePtr);
			}
			_currentContainerPtr = childNodePtr;
		}
	}
	
	long _rootElementPtr = 0;
	
	private class Node {
		private long _objPtr = -1L;
		private final int _nextNodeOffset;
		private int _totalLength;
		public Node(){
			_totalLength = 1;
			_nextNodeOffset = addMemoryHandle();
		}
		
		protected final int addMemoryHandle(){
			int offset = _totalLength;
			_totalLength += ObjectStore.MemoryHandleSize;
			
			return offset;
		}
		
		protected final int addStringRef(){
			int offset = _totalLength;
			_totalLength += StringRefSize;
			
			return offset;
		}

		public final long getPtr(){
			return _objPtr;
		}
		
		public final long newInstance(){
			byte objStoreType = getObjStoreType();
			if(objStoreType >= 0){
				_objPtr = _objectStore.newObject(_totalLength); // allocate the space & get the handle
				_objectStore.setByte(_objPtr, 0, objStoreType); // set the object type
			}else{
				throw new IllegalArgumentException(this.getClass().toString());
			}
			
			initData(); // initialize the data
			
			return getPtr();
		}
		
		protected void initData() {
			setNullToMemoryHandleOffset(_nextNodeOffset);;
		}
		
		protected final byte getObjStoreType(){
			byte objStoreType = -1;
			for(byte index=0; index < ObjectClasses.length; ++index)
				if(ObjectClasses[index] == this.getClass()){
					objStoreType = index;
					break;
				}
			return objStoreType;
		}
		public final Class<?> getClassForInstance(){
			return ObjectClasses[getStoredObjType()];
		}
		
		private final byte getStoredObjType(){
			checkObjectPtr();
			return _objectStore.getByte(_objPtr, 0);
		}

		public final void attachInstance(long index) throws InvalidClassException{
			_objPtr = index;
			boolean inValid = true;
			Class<?> objCls = getClassForInstance();
			while(inValid && objCls != Object.class){
				inValid = (objCls == getClass());
				objCls = objCls.getSuperclass();
			}
			
			if(inValid)
				throw new InvalidClassException(getClass().toString());
		}

		
		private final void checkObjectPtr() {
			if(_objPtr <= 0){
				throw new IllegalStateException("Invalid Node Handle");
			}
		}

		protected final void setNullToMemoryHandleOffset(int offset){
			setMemoryHandleToObjectBuffer(offset, 0);
		}
		protected final void setMemoryHandleToObjectBuffer(int offset, long val){
			checkObjectPtr();
			_objectStore.setObjectHandle(_objPtr, offset, val);
		}
		protected final long getMemoryHandleFromObjectBuffer(int offset){
			checkObjectPtr();
			return _objectStore.getObjectHandle(_objPtr, offset);
		}

		public final static int StringRefSize = 2;
		protected final void setInvalidToStringRefOffset(int offset){
			setStringRefToObjectBuffer(offset, -1);
		}
		protected final void setStringRefToObjectBuffer(int offset, int val){
			checkObjectPtr();
			_objectStore.setIntegerValue(_objPtr, offset, val, StringRefSize);
		}
		protected final int getStringRefFromObjectBuffer(int offset){
			checkObjectPtr();
			return (int)_objectStore.getIntegerValue(_objPtr, offset, StringRefSize);
		}
		
		public final void setNextNodeHandle(long nextNodeHandle){
			setMemoryHandleToObjectBuffer(_nextNodeOffset, nextNodeHandle);;
		}
		
		public final long getNextNodeHandle(){
			return getMemoryHandleFromObjectBuffer(_nextNodeOffset);
		}
	}
	
	private TObjectIntMap<String> StringRefs = new TObjectIntHashMap<>();
	private List<String> StringList = new ArrayList<>();
	private final int InvalidStringRef = 0xffff; 
	private int getStringRef(String val){
		int ref;
		if(StringRefs.containsKey(val))
			ref = StringRefs.get(val);
		else{
			ref = StringRefs.size();
			StringRefs.put(val, ref);
			StringList.add(val);
			
			if(ref >= InvalidStringRef){
				throw new IllegalStateException("Attribute/Tag Name reference has gone beyoned their permissble limit of 65534(0xffff-1).");
			}
		}
		
		return ref;
	}
	private String getStringFromRef(int ref){
		return StringList.get(ref);
	}
	private int getStringRefNoAdd(String val){
		int ref = -1;
		if(StringRefs.containsKey(val))
			ref = StringRefs.get(val);
		
		return ref;
	}
	private class Attribute extends Node{
		private final int _nameIndexOffset;
		private final int _valueIndexOffset;
		
		public final String getName(){
			return getStringFromRef(getStringRefFromObjectBuffer(_nameIndexOffset));
		}
		public final void setName(String name){
			setStringRefToObjectBuffer(_nameIndexOffset, getStringRef(name));
		}
		public final String getValue() {
			return _objectStore.getStringFromByteArray(getMemoryHandleFromObjectBuffer(_valueIndexOffset));
		}
		public final void setValue(String value) {
			setMemoryHandleToObjectBuffer(_valueIndexOffset, _objectStore.appendStringToByteArray(value));
		}
		
		public Attribute() {
			super();
			
			_nameIndexOffset = addStringRef();
			_valueIndexOffset = addMemoryHandle();
		}
		
		@Override
		protected void initData() {
			super.initData();
			setInvalidToStringRefOffset(_nameIndexOffset);
			setNullToMemoryHandleOffset(_valueIndexOffset);
		}
	}
	private class Element extends Node {

		private final int _tagNameOffset;
		private final int _attribOffset;
		private final int _childOffset;
		
		@Override
		protected void initData() {
			super.initData();
			setInvalidToStringRefOffset(_tagNameOffset);
			setNullToMemoryHandleOffset(_attribOffset);
			setNullToMemoryHandleOffset(_childOffset);
		}
		
		public Element() {
			super();
			
			_tagNameOffset = addStringRef();
			_attribOffset = addMemoryHandle();
			_childOffset = addMemoryHandle();
		}
		
		public final String getTagName(){
			return getStringFromRef(getStringRefFromObjectBuffer(_tagNameOffset));
		}
		public final void setTagName(String tagName){
			setStringRefToObjectBuffer(_tagNameOffset, getStringRef(tagName));
		}
		public final int getTagNameRef(){
			return getStringRefFromObjectBuffer(_tagNameOffset);
		}
		
		public final long getChildPtr(){
			return getMemoryHandleFromObjectBuffer(_childOffset);
		}
		public final void setChildPtr(long childPtr){
			setMemoryHandleToObjectBuffer(_childOffset, childPtr);
		}
		
		public final long getAttribPtr(){
			return getMemoryHandleFromObjectBuffer(_attribOffset);
		}
		public final void setAttribPtr(long attribPtr){
			setMemoryHandleToObjectBuffer(_attribOffset, attribPtr);
		}
		
		public boolean isOfType(String tagName, String namespaceName){
			return getTagName().equals(tagName);
		}

		public boolean isValidEndTag(String endNameTag, String endNs){
			return isOfType(endNameTag, endNs);
		}
	}

	private class TextNode extends Node {

		private final int _textPtrOffset;
		public void setText(String text) {
			setMemoryHandleToObjectBuffer(_textPtrOffset, _objectStore.appendStringToByteArray(text));
		}
		public String getText() {
			return _objectStore.getStringFromByteArray(getMemoryHandleFromObjectBuffer(_textPtrOffset));
		}
		
		@Override
		protected void initData() {
			super.initData();
			setNullToMemoryHandleOffset(_textPtrOffset);
		}
		
		public TextNode() {
			super();
			
			_textPtrOffset = addMemoryHandle();
		}
	}

	static final String XPathSplitter = "\\/";
	public long getNodeHandle(String xpath, long nodeHandle) throws InvalidClassException{
		String[] splits = xpath.split(XPathSplitter);
		long retHdl = 0;
		if(splits.length > 0){
			int startIndex = 0;
			if(splits[0].isEmpty()){
				nodeHandle = _rootElementPtr;
				++startIndex;
			}

			Element ele = new Element();
			Node node = new Node();
			Attribute attrib = new Attribute();
			TextNode tn = new TextNode();
			
			int[] nameRefs = new int[splits.length-startIndex];
			for(int index=startIndex; index < splits.length; ++index){
				nameRefs[index-startIndex] = getStringRefNoAdd(splits[index]);
			}
			
			int level=0;
			retHdl = getChildNodeHandle(nodeHandle, level, nameRefs, node, ele, attrib, tn);
		}
		
		return retHdl;
	}

	private long getChildNodeHandle(long nodeHandle, int level, int[] nameRefs, Node node, Element ele, Attribute attrib,
			TextNode tn) throws InvalidClassException {
		long retHdl = nodeHandle;
		if(nameRefs.length > level){
			retHdl = 0;
			node.attachInstance(nodeHandle);
			if(node.getClassForInstance() == Element.class){
				int childRef = nameRefs[level];
				ele.attachInstance(nodeHandle);
				long childPtr = ele.getChildPtr();
				while(childPtr > 0 && retHdl == 0){
					node.attachInstance(childPtr);
					long nextChildPtr = node.getNextNodeHandle();
					if(node.getClassForInstance() == Element.class){
						ele.attachInstance(childPtr);
						if(ele.getTagNameRef() == childRef){
							retHdl = getChildNodeHandle(childPtr, level+1, nameRefs, node, ele, attrib, tn);
						}
					}
					childPtr = nextChildPtr;
				}
			}
		}

		return retHdl;
	}
	
	public class NodeIterator{
		private int[] _nameRefs;
		private long[] _childHandles;
		private long _nodeHandle;
		
		private long _currentHandle;
		private int _level = -1;

		Element _ele = new Element();
		Node _node = new Node();
		Attribute _attrib = new Attribute();
		TextNode _tn = new TextNode();

		private NodeIterator(String xpath, long nodeHandle){
			String[] splits = xpath.split(XPathSplitter);
			int startIndex = 0;
			if(splits.length > 0){
				if(splits[0].isEmpty()){
					nodeHandle = 0;
					++startIndex;
				}
			}
				
			_nameRefs = new int[splits.length-startIndex];
			for(int index=startIndex; index < splits.length; ++index){
				_nameRefs[index-startIndex] = getStringRefNoAdd(splits[index]);
			}
			
			_childHandles = new long[_nameRefs.length];
			_nodeHandle = nodeHandle;
			_currentHandle = 0;
		}
		
		public boolean next() throws InvalidClassException{
			if(_nameRefs.length > 0 && _currentHandle >= 0){ // depth to go or a valid current handle
				while(_level < _nameRefs.length){
					if(_level >= 0){
						long curHandle = _childHandles[_level];
						if(curHandle == 0){ // no more sibling go one level up
							_level --;
							if(_level < 0){
								_currentHandle = -1; // no more valid node. done with iteration
								break;
							}
						}else{
							_node.attachInstance(curHandle);
							_childHandles[_level] = _node.getNextNodeHandle();
							if(_node.getClassForInstance() == Element.class){
								_ele.attachInstance(curHandle);
								if(_ele.getTagNameRef() == _nameRefs[_level]){// satisfy XPath upto this point move ahead to next level
									_level ++;
									if(_level < _nameRefs.length){
										_childHandles[_level] = _ele.getChildPtr();
									}else{
										_currentHandle = curHandle;
										_level --;
										break;
									}
								}
							}
						}
					}else{
						_level = 0;
						if(_nodeHandle > 0){
							_ele.attachInstance(_nodeHandle);
							_childHandles[_level] = _ele.getChildPtr();
						}else{
							_childHandles[_level] = _rootElementPtr;
						}
					}
				}
			}else if(_currentHandle == 0){
				_currentHandle = _nodeHandle; // nothing in xpath so return current handle as next
			}else{
				_currentHandle = -1; // we are done
			}
			return _currentHandle > 0;
		}
		
		public long getCurrentHandle(){
			return _currentHandle;
		}
	}
	
	public NodeIterator getNodeListIterator(String xpath, long nodeHandle){
		return new NodeIterator(xpath, nodeHandle);
	}
	
	Node _nodeGetElementText = new Node();
	Element _eleGetElementText = new Element();
	TextNode _tnGetElementText = new TextNode();
	public void getElementText(long nodeHandle, StringBuilder strBuilder) throws InvalidClassException {
		_nodeGetElementText.attachInstance(nodeHandle);
		if(_nodeGetElementText.getClassForInstance() == TextNode.class){
			_tnGetElementText.attachInstance(nodeHandle);
			strBuilder.append(_tnGetElementText.getText());
		}else {
			_eleGetElementText.attachInstance(nodeHandle);
			long childPtr = _eleGetElementText.getChildPtr();
			while(childPtr > 0){
				getElementText(childPtr, strBuilder);
				_nodeGetElementText.attachInstance(childPtr);
				childPtr = _nodeGetElementText.getNextNodeHandle();
			}
		}
	}

}
