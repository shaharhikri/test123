using System;
using System.IO;
using System.Text;

public sealed class StreamingBmpStream : Stream
{
    private const int FileHeaderSize = 14;
    private const int InfoHeaderSize = 40;
    private const int HeaderSize = FileHeaderSize + InfoHeaderSize;

    private readonly int _width;
    private readonly int _height;
    private readonly long _imageBytes;
    private readonly long _length;
    private readonly int _seed;

    private long _position;

    private readonly byte[] _header; // רק ~54 bytes

    public StreamingBmpStream(int sizeInMb, int seed)
    {
        if (sizeInMb <= 0)
            throw new ArgumentOutOfRangeException(nameof(sizeInMb));

        _seed = seed;

        long targetBytes = sizeInMb * 1024L * 1024L;
        long maxPixels = Math.Max(1, (targetBytes - HeaderSize) / 4);

        _width = (int)Math.Ceiling(Math.Sqrt(maxPixels));
        _height = (int)(maxPixels / _width);
        if (_height < 1) _height = 1;

        _imageBytes = (long)_width * _height * 4;
        _length = HeaderSize + _imageBytes;

        _header = CreateHeader();
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;

    public override long Length => _length;

    public override long Position
    {
        get => _position;
        set => Seek(value, SeekOrigin.Begin);
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (buffer == null)
            throw new ArgumentNullException(nameof(buffer));
        if (offset < 0 || count < 0 || offset + count > buffer.Length)
            throw new ArgumentOutOfRangeException();

        int written = 0;

        while (count > 0 && _position < _length)
        {
            if (_position < HeaderSize)
            {
                buffer[offset++] = _header[_position];
            }
            else
            {
                long pixelIndex = _position - HeaderSize;
                buffer[offset++] = DeterministicByte(_seed, pixelIndex);
            }

            _position++;
            written++;
            count--;
        }

        return written;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        long newPos = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (newPos < 0 || newPos > _length)
            throw new IOException("Invalid seek position");

        _position = newPos;
        return _position;
    }

    public override void Flush() { }

    public override void SetLength(long value) =>
        throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException();

    // -------------------------------------------------
    // Deterministic byte generator
    // -------------------------------------------------
    private static byte DeterministicByte(int seed, long index)
    {
        unchecked
        {
            long x = index ^ seed;
            x = (x * 6364136223846793005L) + 1442695040888963407L;
            return (byte)(x >> 56);
        }
    }

    // -------------------------------------------------
    // BMP Header (54 bytes)
    // -------------------------------------------------
    private byte[] CreateHeader()
    {
        var header = new byte[HeaderSize];
        using var ms = new MemoryStream(header);
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        // BITMAPFILEHEADER
        bw.Write((ushort)0x4D42);          // 'BM'
        bw.Write((uint)_length);
        bw.Write((ushort)0);
        bw.Write((ushort)0);
        bw.Write((uint)HeaderSize);

        // BITMAPINFOHEADER
        bw.Write((uint)40);
        bw.Write(_width);
        bw.Write(-_height);                // top-down
        bw.Write((ushort)1);
        bw.Write((ushort)32);              // 32bpp BGRA
        bw.Write((uint)0);
        bw.Write((uint)_imageBytes);
        bw.Write(2835);
        bw.Write(2835);
        bw.Write((uint)0);
        bw.Write((uint)0);

        return header;
    }
}