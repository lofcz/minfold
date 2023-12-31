namespace Minfold.Annotations;

[Flags]
public enum SqlJsonFlags
{
    None = 1 << 0,
    Instance = 2 << 0,
    List = 3 << 1,
    All = Instance | List | None
}

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class SqlJsonAttribute(SqlJsonFlags flags = SqlJsonFlags.All) : Attribute
{
    public SqlJsonFlags Flags { get; set; } = flags;
}