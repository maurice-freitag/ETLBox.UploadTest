using System.Collections.ObjectModel;
using ETLBox.DataFlow;
using Parquet;
using Parquet.Data;
using Parquet.Schema;

namespace ETLBox.UploadTest;

public partial class ParquetDestinationEx<TInput> : DataFlowStreamDestination<TInput>
{
    private ParquetWriter? writer;
    private readonly GenericTypeInfo typeInfo;
    private readonly ReadOnlyCollection<DataFieldWrapper> fields;
    private readonly Dictionary<string, string> metadata;

    public ParquetDestinationEx(string uri, ResourceType resourceType)
        : this(uri, resourceType, new())
    {
    }

    public ParquetDestinationEx(
        string uri,
        ResourceType resourceType,
        Dictionary<string, string> metadata)
    {
        UseBufferBlock = true;
        Uri = uri;
        ResourceType = resourceType;
        typeInfo = new GenericTypeInfo(typeof(TInput));
        typeInfo.GatherTypeInfo();
        fields = ParquetUtils.PrepareDataFields(typeInfo);
        this.metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
    }

    protected override void CloseStream()
    {
        writer?.Dispose();
        writer = null;
    }

    protected override void InitStream()
    {
    }

    protected override void WriteIntoStream(TInput data)
    {
    }

    protected override void WriteBatch(TInput[] data)
    {
        data = data.Where(d => d != null).ToArray();
        PrepareStreamForBatchProcessing(data);

        writer ??= CreateWriter();

        try
        {
            using var parquetRowGroupWriter = writer.CreateRowGroup();
            foreach (var fieldWrapper in fields)
            {
                var field = fieldWrapper.Field;
                var fieldType = field.IsNullable ? field.ClrNullableIfHasNullsType : field.ClrType;
                var dataArray = Array.CreateInstance(fieldType, data.Length);
                for (var i = 0; i < data.Length; i++)
                {
                    var value = fieldWrapper.GetValue(data[i]);
                    dataArray.SetValue(value, i);
                }
                parquetRowGroupWriter.WriteColumnAsync(new DataColumn(field, dataArray)).ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }
        catch (Exception e)
        {
            ThrowOrRedirectError(e, ErrorSource.ConvertErrorData(data), base.NewMetaDataObject?.ToString());
        }
        LogProgressBatch_AfterThrowOrRedirectError(data.Length);
    }

    private ParquetWriter CreateWriter()
    {
        var schema = new ParquetSchema(fields.Select(w => w.Field).ToArray());
        var w = ParquetWriter.CreateAsync(schema, StreamWriter.BaseStream).ConfigureAwait(false).GetAwaiter().GetResult();
        w.CustomMetadata = metadata;
        return w;
    }
}
