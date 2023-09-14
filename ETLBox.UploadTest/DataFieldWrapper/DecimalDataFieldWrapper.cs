﻿using System.Reflection;
using Parquet.Schema;

namespace ETLBox.UploadTest;

public class DecimalDataFieldWrapper : DataFieldWrapper
{
    public DecimalDataFieldWrapper(DataField field, PropertyInfo propertyInfo)
        : base(field, propertyInfo) { }

    public override object? GetValue(object? obj)
        => base.GetValue(obj) is decimal d ? d.ToString().ToLower() : null;

    public override void SetValue<T>(T? obj, object? value) where T : default
        => base.SetValue(obj, value is string s && decimal.TryParse(s, out var d) ? d : null);
}
