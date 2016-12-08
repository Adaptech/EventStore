
using System;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace EventStore.Core.TransactionLog.Unbuffered
{
    //NOTE THIS DOES NOT SUPPORT ALL STREAM OPERATIONS AS YOU MIGHT EXPECT IT SUPPORTS WHAT WE USE!
    public unsafe class UnbufferedFileStream : Stream
    {
        private byte* _writeBuffer;
        private byte* _readBuffer;
        private readonly int _writeBufferSize;
        private readonly int _readBufferSize;
        private readonly IntPtr _writeBufferOriginal;
        private readonly IntPtr _readBufferOriginal;
        private readonly int _blockSize;
        private long _bufferedCount;
        private bool _aligned;
        private long _lastPosition;
        private bool _needsFlush;
        private SafeFileHandle _handle;
        private long _readLocation = -1;
        private bool _needsRead;

        private UnbufferedFileStream(SafeFileHandle handle, int blockSize, int internalWriteBufferSize, int internalReadBufferSize)
        {
            _handle = handle;
            _readBufferSize = internalReadBufferSize;
            _writeBufferSize = internalWriteBufferSize;
            _writeBufferOriginal = Marshal.AllocHGlobal((int)(internalWriteBufferSize + blockSize));
            _readBufferOriginal = Marshal.AllocHGlobal((int)(internalReadBufferSize + blockSize));
            _readBuffer = Align(_readBufferOriginal, blockSize);
            _writeBuffer = Align(_writeBufferOriginal, blockSize);
            _blockSize = blockSize;
        }

        private byte* Align(IntPtr buf, int alignTo)
        {
            //This makes an aligned buffer linux needs this.
            //The buffer must originally be at least one alignment bigger!
            var diff = alignTo - (buf.ToInt64() % alignTo);
            var aligned = (IntPtr)(buf.ToInt64() + diff);
            return (byte*)aligned;
        }

        public static UnbufferedFileStream Create(string path,
            FileMode mode,
            FileAccess acc,
            FileShare share,
            bool sequential,
            int internalWriteBufferSize,
            int internalReadBufferSize,
            bool writeThrough,
            int minBlockSize)
        {
            int blockSize = (int) NativeFile.GetDriveSectorSize(path);
            blockSize = blockSize > minBlockSize ? blockSize : minBlockSize;
            if (internalWriteBufferSize % blockSize != 0)
                throw new Exception("write buffer size must be aligned to block size of " + blockSize + " bytes");
            if (internalReadBufferSize % blockSize != 0)
                throw new Exception("read buffer size must be aligned to block size of " + blockSize + " bytes");

            var handle = NativeFile.CreateUnbufferedRW(path, acc, share, mode, writeThrough);
            return new UnbufferedFileStream(handle, blockSize, internalWriteBufferSize, internalReadBufferSize);
        }

        public override void Flush()
        {
            CheckDisposed();
            if (!_needsFlush) return;
            var alignedbuffer = (int)GetLowestAlignment(_bufferedCount);
            var positionAligned = GetLowestAlignment(_lastPosition);
            if (!_aligned)
            {
                SeekInternal(positionAligned, SeekOrigin.Begin);
            }
            if (_bufferedCount == alignedbuffer)
            {
                WriteInternal(_writeBuffer, (uint)_bufferedCount);
                _lastPosition = positionAligned + _bufferedCount;
                _bufferedCount = 0;
                _aligned = true;
            }
            else
            {
                var left = _bufferedCount - alignedbuffer;
                WriteInternal(_writeBuffer, (uint)(alignedbuffer + _blockSize));
                _lastPosition = positionAligned + alignedbuffer;
                SetBuffer(alignedbuffer, left);
                _bufferedCount = left;
                _aligned = false;
            }
            _needsFlush = false;
        }

        private static void MemCopy(byte[] src, long srcOffset, byte* dest, long destOffset, long count)
        {
            fixed (byte* p = src)
            {
                MemCopy(p, srcOffset, dest, destOffset, count);
            }
        }

        private static void MemCopy(byte* src, long srcOffset, byte[] dest, long destOffset, long count)
        {
            fixed (byte* p = dest)
            {
                MemCopy(src, srcOffset, p, destOffset, count);
            }
        }

        private static void MemCopy(byte* src, long srcOffset, byte* dest, long destOffset, long count)
        {
            byte* psrc = src + srcOffset;
            byte* pdest = dest + destOffset;

            for (var i = 0; i < count; i++)
            {
                *pdest = *psrc;
                pdest++;
                psrc++;
            }
        }

        private void SeekInternal(long positionAligned, SeekOrigin origin)
        {
            Console.WriteLine("seek to " + positionAligned);
            NativeFile.Seek(_handle, positionAligned, origin);
        }

        private void WriteInternal(byte* buffer, uint count)
        {
            var written = 0; //TODO this needs to loop
            Console.WriteLine("write " + count + " bytes");
            NativeFile.Write(_handle, buffer, count, ref written);
        }

        private int ReadInternal(byte *dest, int offset, int count)
        {
            Console.WriteLine("read " + count + "bytes");
            return NativeFile.Read(_handle, dest, 0, count);
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            long mungedOffset = offset;
            CheckDisposed();
            if (origin == SeekOrigin.Current) throw new NotImplementedException("only supports seek origin begin/end");
            if (origin == SeekOrigin.End) mungedOffset = Length + offset;
            var aligned = GetLowestAlignment(mungedOffset);
            var left = (int)(mungedOffset - aligned);
            Console.WriteLine("external seek called aligned is " + aligned + "base is " + _aligned + " bufferedCount is " + _bufferedCount);
            Flush();
            _bufferedCount = left;
            _aligned = aligned == left;
            _lastPosition = aligned;
            SeekInternal(aligned, SeekOrigin.Begin);
            _needsRead = true;
            return offset;
        }

        private long GetLowestAlignment(long offset)
        {
            return offset - (offset % _blockSize);
        }

        public override void SetLength(long value)
        {
            CheckDisposed();
            var aligned = GetLowestAlignment(value);
            aligned = aligned == value ? aligned : aligned + _blockSize;
            NativeFile.SetFileSize(_handle, aligned);
            Seek(0, SeekOrigin.Begin);
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            Console.WriteLine("read offset " + offset + " count " + count);
            CheckDisposed();
            if (offset < 0 || buffer.Length < offset) throw new ArgumentException("offset");
            if (count < 0 || buffer.Length < count) throw new ArgumentException("offset");
            if (offset + count > buffer.Length)
                throw new ArgumentException("offset + count must be less than size of array");
            var position = GetLowestAlignment(Position);
            var roffset = (int)(Position - position);

            var bytesRead = _readBufferSize;
            if (_readLocation + _readBufferSize <= position || _readLocation > position || _readLocation == -1)
            {
                if(_readLocation + _readBufferSize != position)
                {
                    SeekInternal(position, SeekOrigin.Begin);
                }
                var toRead = _readBufferSize;
                if(count < _readBufferSize) toRead = (int) GetLowestAlignment(count) + _blockSize;
                if(count < _blockSize) toRead = _blockSize;
                Console.WriteLine("reading " + toRead + " bytes buffer size is " + _readBufferSize);
                bytesRead = ReadInternal(_readBuffer, 0, toRead);
                _readLocation = position;
            }
            else if (_readLocation != position)
            {
                roffset += (int)(position - _readLocation);
            }

            var bytesAvailable = bytesRead - roffset;
            if (bytesAvailable <= 0) return 0;
            var toCopy = count > bytesAvailable ? bytesAvailable : count;

            MemCopy(_readBuffer, roffset, buffer, offset, toCopy);
            _bufferedCount += toCopy;
            if (count - toCopy == 0) return toCopy;
            return toCopy + Read(buffer, offset + toCopy, count - toCopy);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            CheckDisposed();
            var done = false;
            long left = count;
            long current = offset;
            if(_needsRead) {
                SeekInternal(_lastPosition, SeekOrigin.Begin);
                ReadInternal(_writeBuffer, 0, (int)_blockSize);
                SeekInternal(_lastPosition, SeekOrigin.Begin);
                _needsRead = false;
            }
            while (!done)
            {
                _needsFlush = true;
                if (_bufferedCount + left < _writeBufferSize)
                {
                    CopyBuffer(buffer, current, left);
                    done = true;
                    current += left;
                }
                else
                {
                    var toFill = _writeBufferSize - _bufferedCount;
                    CopyBuffer(buffer, current, toFill);
                    Flush();
                    left -= toFill;
                    current += toFill;
                    done = left == 0;
                }
            }
        }

        private void CopyBuffer(byte[] buffer, long offset, long count)
        {
            MemCopy(buffer, offset, _writeBuffer, _bufferedCount, count);
            _bufferedCount += count;
        }

        public override bool CanRead
        {
            get
            {
                CheckDisposed();
                return true;
            }
        }

        public override bool CanSeek
        {
            get
            {
                CheckDisposed();
                return true;
            }
        }

        public override bool CanWrite
        {
            get
            {
                CheckDisposed();
                return true;
            }
        }

        public override long Length
        {
            get
            {
                CheckDisposed();
                return NativeFile.GetFileSize(_handle);
            }
        }

        public override long Position
        {
            get
            {
                CheckDisposed();
                if (_aligned)
                    return _lastPosition + _bufferedCount;
                return GetLowestAlignment(_lastPosition) + _bufferedCount;
            }
            set
            {
                CheckDisposed();
                Console.WriteLine("SetPosition called: value is" + value + " lastPosition is " + _lastPosition + " bufferedCount is " + _bufferedCount);
                if(GetLowestAlignment(value) != GetLowestAlignment(_lastPosition))
                    Seek(value, SeekOrigin.Begin);
            }
        }

        private void SetBuffer(long alignedbuffer, long left)
        {
            MemCopy(_writeBuffer, alignedbuffer, _writeBuffer, 0, left);
        }

        [System.Diagnostics.Conditional("DEBUG")]
        private void CheckDisposed()
        {
            //only check in debug
            if (_handle == null) throw new ObjectDisposedException("object is disposed.");
        }

        protected override void Dispose(bool disposing)
        {
            if (_handle == null) return;
            Flush();
            _handle.Close();
            _handle = null;
            _readBuffer = (byte*)IntPtr.Zero;
            _writeBuffer = (byte*)IntPtr.Zero;
            Marshal.FreeHGlobal(_readBufferOriginal);
            Marshal.FreeHGlobal(_writeBufferOriginal);
            GC.SuppressFinalize(this);
        }
    }
}