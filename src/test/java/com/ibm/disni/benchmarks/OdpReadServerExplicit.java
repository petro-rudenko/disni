/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.disni.benchmarks;

import com.ibm.disni.rdma.*;
import com.ibm.disni.rdma.verbs.*;
import org.apache.commons.cli.ParseException;
import sun.nio.ch.FileChannelImpl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;

public class OdpReadServerExplicit implements RdmaEndpointFactory<OdpReadServerExplicit.OdpReadServerEndpoint2> {
	private RdmaActiveEndpointGroup<OdpReadServerEndpoint2> group;
	private String host;
	private int port;
	private int size;
	private int loop;
	private long fileSize;
	private int deviceNum;
	private boolean doPrefetch;

	public OdpReadServerExplicit(String host, int port, int size, int loop,
                                 long fileSize, int deviceNum, boolean doPrefetch) throws IOException{
		this.group = new RdmaActiveEndpointGroup<OdpReadServerEndpoint2>(1, false, 128, 4, 128);
		this.group.init(this);
		this.host = host;
		this.port = port;
		this.size = size;
		this.loop = loop;
		this.fileSize = fileSize;
		this.deviceNum = deviceNum;
		this.doPrefetch = doPrefetch;
	}

	public OdpReadServerEndpoint2 createEndpoint(RdmaCmId id, boolean serverSide)
		throws IOException {
		return new OdpReadServerEndpoint2(group, id, serverSide, size, fileSize, doPrefetch);
	}


	private void run() throws Exception {
		OdpStats stats = new OdpStats(deviceNum);
		System.out.println("ReadServer, size " + size + ", loop " + loop);

		RdmaServerEndpoint<OdpReadServerEndpoint2> serverEndpoint = group.createServerEndpoint();
		InetAddress ipAddress = InetAddress.getByName(host);
		InetSocketAddress address = new InetSocketAddress(ipAddress, port);
		serverEndpoint.bind(address, 10);
		OdpReadServerEndpoint2 endpoint = serverEndpoint.accept();
		System.out.println("ReadServer, client connected, address " + address.toString());

		//let's send a message to the client
		//in the message we include the RDMA information of a local buffer which we allow the client to read using a one-sided RDMA operation
		System.out.println("ReadServer, sending message");
		endpoint.sendMessage();
		//we have to wait for the CQ event, only then we know the message has been sent out
		endpoint.takeEvent();

		//let's wait for the final message to be received. We don't need to check the message itself, just the CQ event is enough.
		endpoint.takeEvent();
		System.out.println("ReadServer, final message");

		//close everything
		endpoint.close();
		serverEndpoint.close();
		group.close();
		stats.printODPStatistics();
	}


	public static void main(String[] args) throws Exception {
		OdpBenchmarkCmdLine cmdLine = new OdpBenchmarkCmdLine("OdpReadServer");
		try {
			cmdLine.parse(args);
		} catch (ParseException e) {
			cmdLine.printHelp();
			System.exit(-1);
		}

		OdpReadServerExplicit server = new OdpReadServerExplicit(cmdLine.getIp(), cmdLine.getPort(),
			cmdLine.getSize(), cmdLine.getLoop(), cmdLine.getFileSize(),
			cmdLine.getDeviceNum(), cmdLine.getDoPrefetch());
		server.run();
	}

	public static class OdpReadServerEndpoint2 extends RdmaActiveEndpoint {
		private ArrayBlockingQueue<IbvWC> wcEvents;

		private ByteBuffer buffers[];
		private IbvMr mrlist[];
		private int buffersize;

		private IbvMr dataMr;
		private IbvMr sendMr;
		private IbvMr recvMr;

		private LinkedList<IbvSendWR> wrList_send;
		private IbvSge sgeSend;
		private LinkedList<IbvSge> sgeList;
		private IbvSendWR sendWR;

		private LinkedList<IbvRecvWR> wrList_recv;
		private IbvSge sgeRecv;
		private LinkedList<IbvSge> sgeListRecv;
		private IbvRecvWR recvWR;

		private long fileSize;
		private boolean doPrefecth;


		protected OdpReadServerEndpoint2(RdmaActiveEndpointGroup<? extends RdmaEndpoint> group, RdmaCmId idPriv,
										boolean serverSide, int size, long fileSize, boolean doPrefetch) throws IOException {
			super(group, idPriv, serverSide);
			this.buffersize = size;
			this.fileSize = fileSize;
			buffers = new ByteBuffer[3];
			this.mrlist = new IbvMr[3];

			for (int i = 0; i < 3; i++){
				buffers[i] = ByteBuffer.allocateDirect(buffersize);
			}

			this.wrList_send = new LinkedList<IbvSendWR>();
			this.sgeSend = new IbvSge();
			this.sgeList = new LinkedList<IbvSge>();
			this.sendWR = new IbvSendWR();

			this.wrList_recv = new LinkedList<IbvRecvWR>();
			this.sgeRecv = new IbvSge();
			this.sgeListRecv = new LinkedList<IbvSge>();
			this.recvWR = new IbvRecvWR();
			this.doPrefecth = doPrefetch;
			this.wcEvents = new ArrayBlockingQueue<IbvWC>(10);

			System.out.println("File size " + fileSize + ", bufferSize: " + size +
				", doPrefetch:" + doPrefetch);
		}

		public void sendMessage() throws IOException {
			this.postSend(wrList_send).execute().free();
		}

		private static final Method mmap;
		private static final Method unmmap;
		static {
			try {
				mmap = FileChannelImpl.class.getDeclaredMethod("map0", int.class, long.class, long.class);
				mmap.setAccessible(true);
				unmmap = FileChannelImpl.class.getDeclaredMethod("unmap0", long.class, long.class);
				unmmap.setAccessible(true);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		private static long roundUpTo4096(long i) {
			return (i + 0xfffL) & ~0xfffL;
		}

		@Override
		protected synchronized void init() throws IOException {
			super.init();
			for (int i = 0; i < 3; i++){
				mrlist[i] = registerMemory(buffers[i]).execute().free().getMr();
			}

			this.dataMr = mrlist[0];
			this.sendMr = mrlist[1];
			this.recvMr = mrlist[2];


			// Create some mmap file
			RandomAccessFile f = new RandomAccessFile("/scrap/users/swat/jenkins/mmap", "rw");
			FileChannel channel = f.getChannel();
			ByteBuffer sendBuf = buffers[1];

			// Put normal MR data
			sendBuf.putLong(dataMr.getAddr());
			sendBuf.putInt(dataMr.getLength());
			sendBuf.putInt(dataMr.getLkey());

			System.out.println("Registering ODP  buffer");
			long fileAddress = 0;

			for (long i = 0; i < 1000; i++) {
				try {
					long start = i*buffersize ;
					long distanceFromPageBoundary = start % 4096;
					long allignOffset = start - distanceFromPageBoundary;
					long alignedLength = buffersize + distanceFromPageBoundary;
					fileAddress = (long)mmap.invoke(channel, 1, allignOffset, alignedLength);
					IbvMr odp = pd.regMr(fileAddress, buffersize, access | IbvMr.IBV_ACCESS_ON_DEMAND).execute().free().getMr();
					if (doPrefecth){
						odp.expPrefetchMr(allignOffset, buffersize);
					}
					sendBuf.putLong(odp.getAddr());
					sendBuf.putInt(odp.getLkey());
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				} catch (InvocationTargetException e) {
					e.printStackTrace();
				}
				//System.out.println("Registered ODP file portion: " + fileAddress);
			}


			/*
			ByteBuffer sendBuf = buffers[1];
			sendBuf.putLong(dataMr.getAddr());
			sendBuf.putInt(dataMr.getLength());
			sendBuf.putInt(dataMr.getLkey());
			sendBuf.clear();
			*/


			sendBuf.clear();
			sgeSend.setAddr(sendMr.getAddr());
			sgeSend.setLength(sendMr.getLength());
			sgeSend.setLkey(sendMr.getLkey());
			sgeList.add(sgeSend);
			sendWR.setWr_id(2000);
			sendWR.setSg_list(sgeList);
			sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
			sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			wrList_send.add(sendWR);

			sgeRecv.setAddr(recvMr.getAddr());
			sgeRecv.setLength(recvMr.getLength());
			int lkey = recvMr.getLkey();
			sgeRecv.setLkey(lkey);
			sgeListRecv.add(sgeRecv);
			recvWR.setSg_list(sgeListRecv);
			recvWR.setWr_id(2001);
			wrList_recv.add(recvWR);

			this.postRecv(wrList_recv).execute();
		}

		public void dispatchCqEvent(IbvWC wc) throws IOException {
			wcEvents.add(wc);
		}

		public IbvWC takeEvent() throws InterruptedException{
			return wcEvents.take();
		}

	}
}
