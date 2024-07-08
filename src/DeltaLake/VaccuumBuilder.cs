using DeltaLake.Protocol;

namespace DeltaLake;

public class VacuumBuilder
{
   // private DeltaTableState _snapshot;
   private readonly DeltaTable _deltaTable;
   private readonly IDeltaFileSystem _fileSystem;
   private TimeSpan? _retentionPeriod;
   private bool _enforceRetentionDuration = true;
   private bool _dryRun;
   // private IClock _clock;

   public VacuumBuilder(DeltaTable deltaTable)
   {
      _fileSystem = deltaTable.FileSystem;
      // _snapshot = snapshot;
      _deltaTable = deltaTable;
      // _clock = new SystemClock
   }

   public VacuumBuilder WithRetentionPeriod(TimeSpan retentionPeriod)
   {
      _retentionPeriod = retentionPeriod;
      return this;
   }

   public VacuumBuilder WithDryRun(bool dryRun)
   {
      _dryRun = dryRun;
      return this;
   }

   public VacuumBuilder WithEnforceRetentionDuration(bool enforce)
   {
      _enforceRetentionDuration = enforce;
      return this;
   }

   // public VacuumBuilder WithClock(IClock clock)
   // {
   //    _clock = clock;
   //    return this;
   // }

   // public async Task<VacuumMetrics> ExecuteAsync()
   public VacuumMetrics ExecuteAsync(VacuumPlan plan)
   {
      // Implementation of the vacuum logic goes here
      // This should include:
      // 1. Calculating the retention period
      // 2. Listing all files in the Delta table's storage
      // 3. Identifying files that can be safely deleted
      // 4. Optionally deleting the files if not a dry run
      // 5. Returning metrics about the operation
      
      var metrics = new VacuumMetrics();
      DateTimeOffset now = DateTime.UtcNow;
      // use now and calculate the retention threshold with the retention period
      DateTimeOffset retentionThreshold = now - (_retentionPeriod ?? TimeSpan.FromDays(7));

      // List all files in the Delta table's storage
      //var allOtherFiles = _deltaTable.Files.Where()
      //var allFiles = await _fileSystem.ListFilesAsync();
      var allFiles = _fileSystem.ListFiles();

      // Identify files that can be safely deleted
      var filesToDelete = allFiles.Where(file => file.LastModified < retentionThreshold).ToList();

      if (!_dryRun)
      {
         // Delete the files
         foreach (var file in filesToDelete)
         {
            //TODO: Implement the delete logic
            // await _fileSystem.Delete(file.Path);
            metrics.FilesDeleted.Add(file.Path);
         }
      }
      else
      {
         // If dry run, just add to metrics without deleting
         foreach (DeltaFileInfo file in filesToDelete)
         {
            metrics.FilesDeleted.Add(file.Path);
         }

         var files = filesToDelete.Select(file => file.Path);
         foreach (var file in files)
         {
            metrics.FilesDeleted.Add(file);
         }
      }

      metrics.DryRun = _dryRun;

      return metrics;
   }
}

/// <summary>
/// Details for the Vacuum operation including which files were deleted
/// </summary>
public class VacuumMetrics
{
   /// <summary>
   /// Was this a dry run
   /// </summary>
   public bool DryRun { get; set; }
   /// <summary>
   /// Files deleted successfully
   /// </summary>
   public DeltaArray<string> FilesDeleted { get; set; } = [];
}

/// <summary>
/// Details for the Vacuum start operation for the transaction log
/// </summary>
public class VacuumStartOperationMetrics
{
   /// <summary>
   /// The number of files will be deleted
   /// </summary>
   long NumberOfFilesToDelte { get; set; }
   /// <summary>
   /// Size of the data to be deleted in Bytes
   /// </summary>
   long SizeOfDataToDelete { get; set; }
}

public class VacuumPlan
{
   public DeltaArray<DeltaFileInfo> FilesToDelete { get; private set; } = [];
}