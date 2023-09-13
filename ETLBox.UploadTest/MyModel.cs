using ETLBox.DataFlow;

namespace ETLBox.UploadTest
{
    public class MyModel
    {
        [IdColumn]
        [DistinctColumn]
        public Guid Id { get; set; }
        public string SomeString { get; set; } = "";
        public DateTime Starttime { get; set; }
        public DateTime Endtime { get; set; }
        public int SomeInt1 { get; set; }
        public int SomeInt2 { get; set; }
        public Guid SomeId1 { get; set; }
        public Guid SomeId2 { get; set; }
        public Guid SomeId3 { get; set; }
        public double SomeDouble1 { get; set; }
        public double SomeDouble2 { get; set; }
        public double SomeDouble3 { get; set; }
        public double SomeDouble4 { get; set; }
        public double SomeDouble5 { get; set; }
        public double SomeDouble6 { get; set; }
        public double SomeDouble7 { get; set; }
        public double SomeDouble8 { get; set; }

        public static string TableName { get; } = "mymodel";
        public static string Sql { get; } =
            $"""
                SELECT 
                    x.Id,
                    x.SomeString,
                    x.Starttime,
                    x.Endtime,
                    x.SomeInt1,
                    x.SomeInt2,
                    x.SomeId1,
                    x.SomeId2,
                    x.SomeId3,
                    x.SomeDouble1,
                    x.SomeDouble2,
                    x.SomeDouble3,
                    x.SomeDouble4,
                    x.SomeDouble5,
                    x.SomeDouble6,
                    x.SomeDouble7,
                    x.SomeDouble8
                FROM 
                    {TableName} AS x
            """;
    }
}