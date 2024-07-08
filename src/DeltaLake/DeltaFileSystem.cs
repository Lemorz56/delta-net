namespace DeltaLake;

public class DeltaFileSystem : IDeltaFileSystem
{
    public string RootPath { get; }
    public DeltaFileSystem(string path) => RootPath = path;
    public bool DirectoryExists(string path) => Directory.Exists(Path.Combine(RootPath, path));
    public void CreateDirectory(string path) => Directory.CreateDirectory(Path.Combine(RootPath, path));
    public bool FileExists(string path) => File.Exists(Path.Combine(RootPath, path));
    public long GetFileSize(string path) => new FileInfo(Path.Combine(RootPath, path)).Length;
    public Stream OpenRead(string path) => File.OpenRead(Path.Combine(RootPath, path));
    public Stream OpenWrite(string path) => File.OpenWrite(Path.Combine(RootPath, path));
    public IEnumerable<string> ReadAllLines(string path) => File.ReadAllLines(Path.Combine(RootPath, path));
    public void WriteFile(string path, IEnumerable<string> content) => File.WriteAllLines(Path.Combine(RootPath, path), content);
    public string CreateTempFile()
    {
        var fileName = $".{Guid.NewGuid()}";
        File.WriteAllText(Path.Combine(RootPath, fileName), "");
        return fileName;
    }
    public bool MoveFile(string source, string destination)
    {
        try
        {
            File.Move(Path.Combine(RootPath, source), Path.Combine(RootPath, destination), false);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    /// <summary>
    /// List all delta files in the given path
    /// </summary>
    /// <returns>IEnumerable of <see cref="DeltaFileInfo"/> </returns>
    /// <exception cref="DirectoryNotFoundException"></exception>
    public IEnumerable<DeltaFileInfo> ListFiles()
    {
        var directory = new DirectoryInfo(RootPath);
        return directory.GetFiles().Select(file => new DeltaFileInfo(file.FullName, file.Length, file.LastWriteTimeUtc));
        // var directory = new DirectoryInfo(Path.Combine(RootPath, path));
        // return !directory.Exists
        //     ? throw new DirectoryNotFoundException($"Directory not found: {directory.FullName}")
        //     : directory.GetFiles().Select(file => new DeltaFileInfo(file.FullName, file.Length, file.LastWriteTimeUtc));
    }
}

//TODO: move to a separate file
public class DeltaFileInfo(string path, long size, DateTimeOffset lastModified) : IEquatable<DeltaFileInfo>
{
    public string Path { get; } = path;
    public long Size { get; } = size;
    public DateTimeOffset LastModified { get; } = lastModified;

    public bool Equals(DeltaFileInfo? other)
    {
        if (ReferenceEquals(null, other)) return false;
        if (ReferenceEquals(this, other)) return true;
        return Path == other.Path && Size == other.Size && LastModified.Equals(other.LastModified);
    }

    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(null, obj)) return false;
        if (ReferenceEquals(this, obj)) return true;
        return obj.GetType() == GetType() && Equals((DeltaFileInfo)obj);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Path, Size, LastModified);
    }
}
