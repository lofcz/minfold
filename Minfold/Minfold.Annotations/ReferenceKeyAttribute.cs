namespace Minfold.Annotations;

[AttributeUsage(AttributeTargets.Property)]
public class ReferenceKeyAttribute : Attribute
{
    public Type Type { get; set; }
    public string Property { get; init; }
    public bool? Enforced { get; init; }
    
    public ReferenceKeyAttribute(Type type, string property)
    {
        Type = type;
        Property = property;
        Enforced = null;
    }
    
    public ReferenceKeyAttribute(Type type, string property, bool enforced)
    {
        Type = type;
        Property = property;
        Enforced = enforced;
    }
}