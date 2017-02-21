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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class ByteBigArrayList { // TODO: as of now this class is not ready for multi threaded applications. We need to do the modification for multithreading
	
	private static Logger logger = LogManager.getLogger();
	
	private static final long TwoByteMask = 0xffffL;	// mask to extract first 2 byte from an integer which is larger than 2 bytes
	private static final long TwoByteStorageSize = TwoByteMask + 1;
	private static final int TwoByteBitSize = 16;
	private static final int ByteArrayHolderSizeIncr = 256;
	
	// memory threshold to be used by all instances of this class cumulatively. this is in percentage of max jvm memory
	private static final long MemoryThreshold = 70;// TODO: should be configurable and should be set to less than 90 for proper function of non byte array dependent part of the application
	
	private boolean _useFilePaging = (MemoryThreshold > 0); // paging is enabled if memory threshold is defined. anything <= 0 is considered as undefined
	
	private long _currentSize = 0;
	private int _holderUsesSize = 0;
	private long _currentCapacity = 0;
	private DataPage[] _dataPages = new DataPage[ByteArrayHolderSizeIncr];
	private static volatile long _accessCounter = 0;
	
	public ByteBigArrayList() {
		if(_useFilePaging){
			try {
				_swapFilePath = new File(String.format("./temp/%s.swp", UUID.randomUUID()));
				_swapFile = new RandomAccessFile(_swapFilePath, "rw");
				_swapFilePath.deleteOnExit(); // just to make sure that this file will get deleted when jvm exits
			} catch (FileNotFoundException e) {
				_swapFile = null;
				_swapFilePath = null;
				_useFilePaging = false;
				
				logger.error(e);
				logger.info("File paging disabled because of previous exception.");
			}
		}
	}
	
	public final long size(){
		return _currentSize;
	}
	
	public void trimToSize(){
		// removed the pages which are not used at all
		while(_currentSize+TwoByteStorageSize <= _currentCapacity){
			_currentCapacity -= TwoByteStorageSize;
			_holderUsesSize --;
			_dataPages[_holderUsesSize] = null;
		}
	}
	
	public final byte[] toArray(long index, int length){
		byte[] bytes = new byte[length];
		int startIndex = 0;
		while(length > 0){
			DataPage curDataPage = getDataPageForIndex(index);
			int arrIndex = getArrayIndex(index);
			int tempLen = curDataPage.copyToBytes(arrIndex, bytes, startIndex, length); 
			length -= tempLen;
			startIndex += tempLen;
			index += tempLen;
		}
		
		return bytes;
	}
	
	public final byte get(long index){
		if(index >= 0 && index < _currentSize){
			DataPage curDataPage = getDataPageForIndex(index);
			return curDataPage.getAt(getArrayIndex(index));
		}else{
			throw new IndexOutOfBoundsException();
		}
	}

	public final void set(long index, byte byteValue){
		if(index >= 0 && index < _currentSize){
			DataPage curPage = getDataPageForIndex(index);
			curPage.setAt(getArrayIndex(index), byteValue);
		}else{
			throw new IndexOutOfBoundsException();
		}
	}
	
	public final long add(byte[] bytes, int startIndex, int length){
		ensureStorage(length);
		
		long index = _currentSize;
		while(length > 0){
			DataPage curPage = getDataPageForIndex(_currentSize);
			int arrIndex = getArrayIndex(_currentSize);
			int tempLen = curPage.copyFromBytes(arrIndex, bytes, startIndex, length);
			length -= tempLen;
			startIndex += tempLen;
			_currentSize += tempLen;
		}
		
		return index;
	}
	
	public final long add(byte[] bytes){
		return add(bytes, 0, bytes.length);
	}
	
	public final long add(byte byteValue){
		ensureStorage(1);
		long index = _currentSize;
		_currentSize ++;
		set(index, byteValue);
		
		return index;
	}
	
	public final long allocateSpace(int size){
		long index = _currentSize;
		ensureStorage(size);
		_currentSize += size;
		
		return index;
	}
	private final int getArrayIndex(long index) {
		return (int)(index & TwoByteMask);
	}
	private final void ensureStorage(long additionalStorageReqr) {
		long newRequiredSize = _currentSize + additionalStorageReqr;
		while(_currentCapacity < newRequiredSize){
			int holderSize = getListHolderIndex(newRequiredSize-1)+1;
			if(_dataPages.length < holderSize){
				int newHolderLen = (holderSize/ByteArrayHolderSizeIncr + 1)*ByteArrayHolderSizeIncr;
				_dataPages = Arrays.copyOf(_dataPages, newHolderLen);
			}
			_dataPages[_holderUsesSize++] = new DataPage(); // add 64KB
			_currentCapacity += TwoByteStorageSize;
		}
	}
	private final int getListHolderIndex(long index) {
		return (int)(index >> TwoByteBitSize); // most significant 2 bytes
	}
	private final DataPage getDataPageForIndex(long index){
		return _dataPages[getListHolderIndex(index)];
	}
	
	private final byte[] createPageData(){
		return new byte[(int)TwoByteStorageSize];
	}
	
	RandomAccessFile _swapFile = null;
	File _swapFilePath = null;
	@Override
	protected void finalize() throws Throwable {
		if(_swapFile != null){
			_swapFile.close();
			_swapFilePath.delete();
			_swapFile = null;
		}
		
		super.finalize();
	}

	private final class DataPage {
		long _accessNo = 0;
		long _swapPos = -1;
		boolean _dirty = false; // to signify if the paged file data and in memory data are not in sync
		private byte[] _data = null; // don't use _data directly unless there is a reason it should be used via getData() wrapper

		public DataPage(){
			assignPageData(createPageData());
		}
		
		public final byte getAt(int index) {
			{//synchronized (this) {
				return getData()[index];
			}
		}
		public final void setAt(int index, byte byteValue) {
			{//synchronized (this) {
				getData()[index] = byteValue;
				_dirty = true;
			}
		}

		public final int copyToBytes(int index, byte[] destBytes, int destStartIndex, int length) { // copy data to a byte array
			int allowedLen = Math.min(length, (int)(TwoByteStorageSize-index)); // length of data which can be copied from this page
			{//synchronized (this) {
				System.arraycopy(getData(), index, destBytes, destStartIndex, allowedLen);
			}
			return allowedLen;
		}
		
		public final int copyFromBytes(int index, byte[] srcBytes, int srcStartIndex, int length) {
			int allowedLen = Math.min(length, (int)(TwoByteStorageSize-index)); // length of data which can be copied from this page
			{//synchronized (this) {
				System.arraycopy(srcBytes, srcStartIndex, getData(), index, allowedLen);

				_dirty = true;
			}
			return allowedLen;
		}

		public final void swap(double thresKeepPriority) {
			{//synchronized (this) {
				if(getKeepPriority() <= thresKeepPriority){
					if(_dirty){ // write to disk only if in memory and on disk are not in sync
						try {
							if(_swapPos < 0){
								_swapPos = _swapFile.length();
							}
							_swapFile.seek(_swapPos);
							_swapFile.write(_data);
							_dirty = false; // data in sync => dirty is false
						} catch (IOException e) {
							// we are logging this error but ignoring any action since this will not prevent the application
							// from running unless it is out of memory so we will continue until out of memory
							logger.warn(e);   
						}
					}
					if(!_dirty){
						_data = null; // it is safe to set data null only if data is synced to disk
					}
				}
			}
		}

		public final double getKeepPriority() {
			return (isInMemory() ? _accessNo : Double.MAX_VALUE);
		}
		public final boolean isInMemory() {
			{//synchronized (this) {
				return (_data != null);
			}
		}
		private final byte[] getData(){
			ensureDataAllocation();
			
			return _data;
		}
		private final void ensureDataAllocation() {
			if(_useFilePaging){
				_accessNo = (++ _accessCounter);
				if(_data == null){
					try {
						byte[] data = createPageData();
						_swapFile.seek(_swapPos);
						_swapFile.read(data);
						assignPageData(data);
					} catch (IOException e) {
						// we are ignoring this exception and let the application get NPE later
						logger.warn(e);
						logger.warn("System is going to get NPE becuase of the excpetion logged above.");
					}
				}
			}
		}

		private final void assignPageData(byte[] data) {
			_data = data;
			if(_useFilePaging){
				_accessNo = (++ _accessCounter);
				_pagingController.addDataPage(this);
			}
		}
	}

	private final static class PagingController { // TODO: for async processing we can use a thread pool of single thread and can be launched as on required basis
		private static final int magicPageCount;
		private static final int maxInMemPageCount;
		static {
			Runtime rt = Runtime.getRuntime();
			maxInMemPageCount = (int)((MemoryThreshold*rt.maxMemory())/(100*TwoByteStorageSize));
			int configuredMagicPageCount = 127;  // TODO: should be configurable. number of pages to swap and no of pages after which memory check will happen
			magicPageCount = Math.min(maxInMemPageCount/2, configuredMagicPageCount); // MagicPageCount should be at max half of maxInMemPageCount
		}
		
		private ArrayList<WeakReference<DataPage>> _inMemoryPages = new ArrayList<>();

		public void addDataPage(DataPage dataPage) {
			WeakReference<DataPage> weakRef = new WeakReference<ByteBigArrayList.DataPage>(dataPage);
			{//synchronized (this) {
				_inMemoryPages.add(weakRef);
			}
			if(_inMemoryPages.size() % magicPageCount == 0){
				runPagingIfRequired();
			}
		}
		private int clearUnqualified(){
			{//synchronized (this) {
				_inMemoryPages.removeIf(wr -> !isQualified(wr.get()));
			}
			
			return _inMemoryPages.size();
		}
		private boolean isQualified(DataPage dataPage) {
			return (dataPage != null && dataPage.isInMemory()); // only in memory data pages are qualified for paging
		}
		
		private /*synchronized */void runPagingIfRequired() {
			if(_inMemoryPages.size() > 0){
				if(maxInMemPageCount < _inMemoryPages.size() // threshold is reached 
						&& maxInMemPageCount < clearUnqualified()){ // threshold is reached even after clean up
					List<DataPage> inMemoryPages = null;
					{//synchronized (this) {
						inMemoryPages = _inMemoryPages.stream().map(wr->wr.get()).filter(dp-> isQualified(dp)).collect(Collectors.toList());
					}

					int pagesToPersist = Math.max(inMemoryPages.size() - maxInMemPageCount, magicPageCount);
					if(pagesToPersist > 0){
						List<Double> priorityList = inMemoryPages.stream().map(dp->dp.getKeepPriority()).sorted().collect(Collectors.toList());
						double thresKeepPriority = (priorityList.get(pagesToPersist-1));
						logger.debug("Freeing {} pages.", pagesToPersist);
						for(DataPage dp:inMemoryPages){
							dp.swap(thresKeepPriority);
						}
						clearUnqualified(); // clean the list
					}				

				}
				
				logger.debug("Total occupied memory by byte arrays: {} MB", (_inMemoryPages.size()*TwoByteStorageSize/(1024*1024)));
			}
		}
	}

	static final PagingController _pagingController = new PagingController();
}
