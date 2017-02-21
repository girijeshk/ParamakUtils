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

import java.io.UnsupportedEncodingException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gnu.trove.list.array.TLongArrayList;

public class ObjectStore {
	private final static Logger logger = LogManager.getLogger();
	
	private final static String CurrentCharset = "UTF-8";
	private final static int HashSize = 31607;
	private final ByteBigArrayList _objectStore = new ByteBigArrayList();
	public ObjectStore(){
		_objectStore.add((byte)0); // add a 0 byte so that 0 is always an invalid position
	}
	
	private TLongArrayList[] _stringHashes = new TLongArrayList[HashSize];
	private static int calculateByteArrayHash(byte[] byteArray){
		int len = byteArray.length;
		int hashCode = 0;
		if(len > 0){
			for(int i=0; i<7; ++i){
				int index = (i*len/7)%len;
				int byteVal = byteArray[index];
				byteVal = i%2==0 ? (byteVal & 0xf) : ((byteVal >> 4) & 0xf);
				hashCode  = (hashCode << 4) | byteVal;
			}
		}
		return hashCode % HashSize;
	}

	private final static long StringCheckMask = 0x000000ff00000000L;
	public String getStringFromByteArray(long index) {
		byte[] strData = {0, 0, 0, 0};
		int stringLen = 0;
		if((index & StringCheckMask) != StringCheckMask){
			ByteBigArrayList stringStore = _objectStore;
			long[] lengthIndex = readVarIntFromByteArray(index, stringStore);
			strData = stringStore.toArray(lengthIndex[1], (int)lengthIndex[0]);
			stringLen = strData.length;
		}else{
			index = (index & 0xffffffffL);
			int len=0;
			while((index & 0xff)!=0){// get the string start
				strData[len ++] = (byte)(index & 0xff);
				index = index >> 8;
			}
			stringLen = len;
		}
		
		String str = null;
		try {
			str = new String(strData, 0, stringLen, CurrentCharset);
		} catch (UnsupportedEncodingException e) {
			logger.error("This should never happen. Why UTF-8 is not supported???", e);
		}
		
		return str;
	}
	boolean _noStringHashing = true;
	public long appendStringToByteArray(String str) {
		long index = 0;
		
		if(str != null){
			byte[] strBytes = null;
			try {
				strBytes = str.getBytes(CurrentCharset);
			} catch (UnsupportedEncodingException e) {
				logger.error("This should never happen. Why UTF-8 is not supported???", e);
			}
			if(strBytes.length > 4){
				ByteBigArrayList stringStore = _objectStore;
				if(_noStringHashing){
					index = stringStore.size();
				}else{
					int hashCode = calculateByteArrayHash(strBytes);
					TLongArrayList hashCollisionList = _stringHashes[hashCode];
					if(hashCollisionList == null){
						_stringHashes[hashCode] = hashCollisionList = new TLongArrayList();
					}

					// find the index of the string in the hashCollision list
					index = findOrAdd(strBytes, stringStore, hashCollisionList, stringStore.size());
				}
				
				if(index == stringStore.size()){ // index is set to end of string store; means this string is new and need to be added to end of the buffer
					addVarIntToByteArray(strBytes.length, stringStore);
					stringStore.add(strBytes);
				}				//
			}else{
				int bytesAsInt = 0;
				int len = 0;
				for(byte val:strBytes){
					bytesAsInt = bytesAsInt << 8;
					bytesAsInt = (bytesAsInt | val);
					len++;
				}
				while(len < 4){
					++len;
					bytesAsInt = bytesAsInt << 8;
				}
				bytesAsInt = Integer.reverseBytes(bytesAsInt);
				index = (StringCheckMask | bytesAsInt);
			}

		}
		
		return index;
	}
	private static long findOrAdd(byte[] strBytes, ByteBigArrayList stringStore, TLongArrayList hashCollisionList, long newStrIndex) {
		long strIndex = newStrIndex;
		if(hashCollisionList.isEmpty()){
			hashCollisionList.add(newStrIndex);
		}else{
			// binary search
			int startHashIndex = 0, lastHashIndex = hashCollisionList.size()-1;
			while(startHashIndex <= lastHashIndex){
				int middleHashIndex = (startHashIndex + lastHashIndex)/2;
				long curStrIndex = hashCollisionList.get(middleHashIndex);
				int compareVal = compareByteArray(strBytes, stringStore, curStrIndex);
				if(compareVal > 0){
					startHashIndex = middleHashIndex + 1;
				}else if(compareVal < 0){
					lastHashIndex = middleHashIndex - 1;
				}else {
					strIndex = curStrIndex;
					break;
				}
			}
			
			if(startHashIndex > lastHashIndex){
				if(startHashIndex == hashCollisionList.size()){
					hashCollisionList.add(newStrIndex);
				}else{
					hashCollisionList.insert(startHashIndex, newStrIndex);
				}
			}
		}
		return strIndex;
	}	
	private static int compareByteArray(byte[] strBytes, ByteBigArrayList stringStore, long index) {
		long[] lengthIndex = readVarIntFromByteArray(index, stringStore);
		long storedLen = lengthIndex[0], storedIndex = lengthIndex[1];
		int rc = strBytes.length - (int)storedLen; // 0 if all is same, difference of length followed difference of first differing byte
		for(int i=0; i<storedLen && rc==0; ++i){ // no need to check length of other since rc==0 will take care of that
			rc = (0xff & strBytes[i]) - (0xff & stringStore.get(i+storedIndex));
		}
		return rc;
	}
	final static long byteMask = 0x7fL;
	final static long additiveMask = 0x80L;
	final static int bitCount = 7;
	private static void addVarIntToByteArray(long val, ByteBigArrayList byteArray){
		do{
			long byteValue = val & byteMask;
			val = val >> bitCount;
			
			if(val != 0){
				byteValue = byteValue | additiveMask;
			}
			
			byteArray.add((byte)byteValue);
		}while(val != 0);
	}
	private static long[] readVarIntFromByteArray(long index, ByteBigArrayList byteArray){
		boolean getNext = true;
		long val = 0;
		int shiftBits = 0;
		while(getNext){
			int byteVal = byteArray.get(index++);
			val = val | ((byteVal & byteMask) << shiftBits);
			if((byteVal & additiveMask) == 0){
				getNext = false;
			}
			
			shiftBits += bitCount;
		}
		
		return new long[]{val, index}; // return new index and read value
	}
	public void compact() {
		_objectStore.trimToSize();
	}
	public void addDebugInfo(StringBuilder strBuilder) {
		long nonNullEntries = 0;
		long numStr = 0;
		long maxCollision = 0;
		for(int i=0; i<_stringHashes.length;++i)
			if(_stringHashes[i] != null){
				++nonNullEntries;
				numStr += _stringHashes[i].size();
				if(_stringHashes[i].size() > 16)
				maxCollision += 1;//Math.max(maxCollision, _stringHashes[i].size());
			}
		strBuilder.append(String.format("Object buffer=%d   hash efficiency=[%d/%d/%d/%d]"
				, _objectStore.size(), nonNullEntries, _stringHashes.length, numStr, maxCollision));
	}
	
	public long newObject(int objectSize){
		return _objectStore.allocateSpace(objectSize);
	}
	public void setByte(long objectHandle, int offset, byte byteVal) {
		_objectStore.set(objectHandle+offset, byteVal);
	}
	public byte getByte(long objectHandle, int offset) {
		return _objectStore.get(objectHandle+offset);
	}
	
	public final static long ByteMask = 0xffL;
	public final static int ByteBits = 8;
	public static final int MemoryHandleSize = 5;
	public final void setIntegerValue(long objectHandle, int offset, long val, int sizeOfInt){
		for(int i=0; i<sizeOfInt; ++i){
			_objectStore.set(objectHandle+offset+i, (byte)(val & ByteMask));
			val = val >> ByteBits;
		}
	}
	public final long getIntegerValue(long objectHandle, int offset, int sizeOfInt){
		long val = 0;
		for(int i=sizeOfInt-1; i>=0; --i){
			val = val << ByteBits;
			byte byteVal = _objectStore.get(objectHandle+offset+i);
			val = val | (byteVal & ByteMask) ;
		}
		return val;
	}
	
	public final void setObjectHandle(long objectHandle, int offset, long handleValue){
		setIntegerValue(objectHandle, offset, handleValue, MemoryHandleSize);
	}
	public final long getObjectHandle(long objectHandle, int offset){
		return getIntegerValue(objectHandle, offset, MemoryHandleSize);
	}
	public final long getHandleAtOffset(long objectHandle, int offset){
		return objectHandle + offset;
	}
}
