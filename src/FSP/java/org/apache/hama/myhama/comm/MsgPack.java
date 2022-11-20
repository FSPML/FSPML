package org.apache.hama.myhama.comm;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hama.myhama.api.MsgRecord;
import org.apache.hama.myhama.api.UserTool;

public class MsgPack<V, W, M, I> implements Writable {
	private MsgRecord<M>[] msgData;
	private int size; //#target vertices
	private ByteArrayOutputStream bos;
	private DataInputStream inputStream;
	private UserTool<V, W, M, I> userTool;
	private long io_byte; //io bytes during pulling messages based on edges
	private long io_pull_vert; //io bytes of reading source vertices when pulling messages
	private Long edge_read = 0L;
	private Long fragment_read = 0L;
	private Long msg_pro = 0L;
	private Long msg_rec = 0L;
	
	private boolean over = false;
	
	public MsgPack() {
		this.io_byte = 0L;
		this.io_pull_vert = 0L;
	}
	
	public MsgPack(UserTool<V, W, M, I> _userTool) {
		this.userTool = _userTool;
		this.io_byte = 0L;
		this.io_pull_vert = 0L;
	}
	
	public void setUserTool(UserTool<V, W, M, I> _userTool) {
		this.userTool = _userTool;
	}
	
	public void setOver() {
		this.over = true;
	}
	
	public boolean isOver() {
		return this.over;
	}
	
	/**
	 * Set info. of edges, even though no messages are produced.
	 * @param _io
	 * @param _edgeNum
	 * @param _fragNum
	 */
	public void setEdgeInfo(long _io, long _io_vert, long _edgeNum, long _fragNum) {
		this.io_byte = _io;
		this.io_pull_vert = _io_vert;
		this.edge_read= _edgeNum;
		this.fragment_read = _fragNum;
	}
	
	/**
	 * Set messages and relative variables for local task.
	 * @param msgData
	 * @param _size
	 * @param _msgProNum
	 * @param _msgRecNum
	 */
	public void setLocal(MsgRecord<M>[] msgData, int _size,
			long _msgProNum, long _msgRecNum) {
		this.msgData = msgData;
		this.size = _size;
		this.msg_pro = _msgProNum;
		this.msg_rec = _msgRecNum;
	}
	
	/**
	 * Set messages in outputstream and relative variables for remote tasks.
	 * @param _bos
	 * @param _size
	 * @param _msgProNum
	 * @param _msgRecNum
	 */
	public void setRemote(ByteArrayOutputStream _bos, int _size,
			long _msgProNum, long _msgRecNum) {
		this.bos = _bos;
		this.size = _size;
		this.msg_pro = _msgProNum;
		this.msg_rec = _msgRecNum;
	}
	
	public MsgRecord<M>[] get() throws IOException {
		if (this.size > 0 && this.msgData == null) {
			deserialize();
		}
		
		return this.msgData;
	}
	
	public int size() {
		return this.size;
	}
	
	public long getIOByte() {
		return this.io_byte;
	}
	
	public long getIOByteOfVertInPull() {
		return this.io_pull_vert;
	}
	
	public long getReadEdgeNum() {
		return this.edge_read;
	}
	
	public long getReadFragNum() {
		return this.fragment_read;
	}
	
	public long getMsgProNum() {
		return this.msg_pro;
	}
	
	public long getMsgRecNum() {
		return this.msg_rec;
	}
	
	private void deserialize() throws IOException {
		/*this.msgData = (MsgRecord<M>[]) new MsgRecord[this.size];
		for (int index = 0; index < this.size; index++) {
			MsgRecord<M> msgRecord = this.userTool.getMsgRecord();
			msgRecord.deserialize(this.inputStream);
			this.msgData[index] = msgRecord;
		}
		this.inputStream = null;*/
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.io_byte = in.readLong();
		this.io_pull_vert = in.readLong();
		this.edge_read = in.readLong();
		this.fragment_read = in.readLong();
		this.msg_pro = in.readLong();
		this.msg_rec = in.readLong();
		this.over = in.readBoolean();
		
		this.size = in.readInt();
		int bytesLength = in.readInt();
		byte[] b = new byte[bytesLength];
		
		in.readFully(b);
		ByteArrayInputStream bytes = new ByteArrayInputStream(b);
		this.inputStream = new DataInputStream(bytes);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.io_byte);
		out.writeLong(this.io_pull_vert);
		out.writeLong(this.edge_read);
		out.writeLong(this.fragment_read);
		out.writeLong(this.msg_pro);
		out.writeLong(this.msg_rec);
		out.writeBoolean(this.over);
		
		out.writeInt(this.size);
		if (this.size == 0) {
			this.bos = new ByteArrayOutputStream(this.size);
		}
		byte[] b = this.bos.toByteArray();
		out.writeInt(b.length);
		out.write(b);
		this.msgData = null;
	}
}