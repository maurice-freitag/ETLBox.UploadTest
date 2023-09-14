using System.Collections.ObjectModel;
using System.Reflection;
using ETLBox.DataFlow;
using ETLBox.DataFlow.Connectors;
using Parquet.Schema;

namespace ETLBox.UploadTest;

public static class ParquetUtils
{
    public static HashSet<Type> SupportedTypes { get; }

    static ParquetUtils()
    {
        SupportedTypes = new ParquetSource().SupportedDataTypes.ToHashSet();
    }

    public static ReadOnlyCollection<DataFieldWrapper> PrepareDataFields(GenericTypeInfo typeInfo)
    {
        var fieldWrappers = new List<DataFieldWrapper>();
        foreach (var prop in typeInfo.Properties)
        {
            if (IgnoreProperty(prop))
                continue;

            DataFieldWrapper field;
            var underlyingType = typeInfo.UnderlyingPropType[prop];
            var isNullable = Nullable.GetUnderlyingType(prop.PropertyType) != null;
            if (underlyingType == typeof(Guid))
                field = new GuidDataFieldWrapper(new DataField(prop.Name, typeof(string)), prop);
            else if (underlyingType == typeof(decimal))
                field = new DecimalDataFieldWrapper(new DataField(prop.Name, typeof(string)), prop);
            else if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTimeOffset))
                field = new DataFieldWrapper(new DateTimeDataField(prop.Name, DateTimeFormat.DateAndTime, isNullable), prop);
            else if (underlyingType == typeof(string))
                field = new DataFieldWrapper(new DataField(prop.Name, typeof(string)), prop);
            else if (SupportedTypes.Contains(underlyingType))
                field = new DataFieldWrapper(new DataField(prop.Name, underlyingType), prop);
            else
                throw new NotSupportedException($"DataType {prop.PropertyType} is not supported.");
            fieldWrappers.Add(field);
        }
        return fieldWrappers.ToArray().AsReadOnly();
    }

    private static bool IgnoreProperty(PropertyInfo prop)
    {
        return !(prop.GetGetMethod() is { } gm)
            || prop.GetSetMethod() is null
            || gm.IsStatic;
    }
}
