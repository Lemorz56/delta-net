using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using DeltaLake.Tests.Unit;

namespace DeltaLake.Tests.Functional;

public class VacuumTableTests
{
   public static Schema TestSchema = new([
      new Field("id", Int32Type.Default, false, []),
      new Field("name", StringType.Default, true, [])
   ], []);

   internal readonly record struct TestTable(int Id, string? Name) : ITable<TestTable>
   {
      public static Schema Schema => TestSchema;

      public static IEnumerable<TestTable> Enumerate(RecordBatch batch)
      {
         for (int i = 0; i < batch.Length; i++)
         {
            var idArray = batch.Column(0) as IReadOnlyList<int?> ?? throw new Exception("Expected non-null array");
            var nameArray = batch.Column(1) as IReadOnlyList<string?> ?? throw new Exception("Expected non-null array");
            yield return new TestTable()
            {
               Id = idArray[i] ?? throw new Exception("Cannot be null"),
               Name = nameArray[i]
            };
         }
      }
   }
   
   [Fact]
   public void CreateTableWithData()
   {
      using var fs = new TestFileSystem();
      var schema = new Schema([
         new("id", Int32Type.Default, false, []),
         new("name", StringType.Default, true, [])
      ], []);
      using var expected = new RecordBatch(schema, [
         new Int32Array.Builder().Append(1).Append(2).Append(3).Build(),
         new StringArray.Builder().Append("one").AppendNull().Append("two").Build()
      ], 3);

      var table = new DeltaTable.Builder()
         .WithFileSystem(fs)
         .WithSchema(schema)
         .Add(expected.Clone())
         .EnsureCreated()
         .Build();
      var actual = table.GetRecordBatches();

      Assert.Equal([expected], actual, new RecordBatchEqualityComparer());

      // maybe not run on FS but the table instead?
      // not sure how to do with the state.
      var vacuumMetrics = new VacuumBuilder(table)
         // .WithRetentionPeriod(TimeSpan.FromDays(7))
         .WithRetentionPeriod(TimeSpan.FromSeconds(0))
         .WithDryRun(false)
         .ExecuteAsync();

      Console.WriteLine(vacuumMetrics.FilesDeleted.Count);
   }
}