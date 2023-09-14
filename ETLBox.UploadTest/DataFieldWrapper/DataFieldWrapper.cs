using System.Reflection;
using Parquet.Schema;

namespace ETLBox.UploadTest;

public class DataFieldWrapper
{
    public DataField Field { get; }
    public PropertyInfo PropertyInfo { get; }

    public DataFieldWrapper(DataField field, PropertyInfo propertyInfo)
    {
        Field = field ?? throw new ArgumentNullException(nameof(field));
        PropertyInfo = propertyInfo ?? throw new ArgumentNullException(nameof(propertyInfo));
    }

    public virtual object? GetValue(object? obj)
        => PropertyInfo.GetValue(obj);

    public virtual void SetValue<T>(T? obj, object? value)
        => PropertyInfo.SetValue(obj, value);
}
