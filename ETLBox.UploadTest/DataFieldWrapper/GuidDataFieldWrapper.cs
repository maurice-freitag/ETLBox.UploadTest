using System.Reflection;
using Parquet.Schema;

namespace ETLBox.UploadTest;

public class GuidDataFieldWrapper : DataFieldWrapper
{
    public GuidDataFieldWrapper(DataField field, PropertyInfo propertyInfo)
        : base(field, propertyInfo) { }

    public override object? GetValue(object? obj)
        => base.GetValue(obj) is Guid guid ? guid.ToString().ToLower() : null;

    public override void SetValue<T>(T? obj, object? value) where T : default
        => base.SetValue(obj, value is string b ? new Guid(b) : null);
}
