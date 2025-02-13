namespace Minfold;

public static class SqlDbTypeExtHelpers
{
    /// <summary>
    /// From highest to lowest
    /// </summary>
    public static readonly Dictionary<SqlDbTypeExt, int> SqlDbTypeExtPrecedence = new Dictionary<SqlDbTypeExt, int>
    {
        {SqlDbTypeExt.Udt, 28},
        {SqlDbTypeExt.Xml, 27},
        {SqlDbTypeExt.DateTimeOffset, 26},
        {SqlDbTypeExt.DateTime2, 25},
        {SqlDbTypeExt.DateTime, 24},
        {SqlDbTypeExt.SmallDateTime, 23},
        {SqlDbTypeExt.Date, 22},
        {SqlDbTypeExt.Time, 21},
        {SqlDbTypeExt.Float, 20},
        {SqlDbTypeExt.Real, 19},
        {SqlDbTypeExt.Decimal, 18},
        {SqlDbTypeExt.Money, 17},
        {SqlDbTypeExt.SmallMoney, 16},
        {SqlDbTypeExt.BigInt, 15},
        {SqlDbTypeExt.Int, 14},
        {SqlDbTypeExt.SmallInt, 13},
        {SqlDbTypeExt.TinyInt, 12},
        {SqlDbTypeExt.Bit, 11},
        {SqlDbTypeExt.NText, 10},
        {SqlDbTypeExt.Text, 9},
        {SqlDbTypeExt.Image, 8},
        {SqlDbTypeExt.Timestamp, 7},
        {SqlDbTypeExt.UniqueIdentifier, 6},
        {SqlDbTypeExt.NVarChar, 5},
        {SqlDbTypeExt.NChar, 4},
        {SqlDbTypeExt.VarChar, 3},
        {SqlDbTypeExt.Char, 2},
        {SqlDbTypeExt.VarBinary, 1},
        {SqlDbTypeExt.Binary, 0}
    };   
}

public enum SqlDbTypeExt
{
    /// <summary>
    /// <see cref="T:System.Int64" />. A 64-bit signed integer.</summary>
    BigInt = 0,
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. A fixed-length stream of binary data ranging between 1 and 8,000 bytes.</summary>
    Binary = 1,
    /// <summary>
    /// <see cref="T:System.Boolean" />. An unsigned numeric value that can be 0, 1, or <see langword="null" />.</summary>
    Bit = 2,
    /// <summary>
    /// <see cref="T:System.String" />. A fixed-length stream of non-Unicode characters ranging between 1 and 8,000 characters.</summary>
    Char = 3,
    /// <summary>
    /// <see cref="T:System.DateTime" />. Date and time data ranging in value from January 1, 1753 to December 31, 9999 to an accuracy of 3.33 milliseconds.</summary>
    DateTime = 4,
    /// <summary>
    /// <see cref="T:System.Decimal" />. A fixed precision and scale numeric value between -10 38 -1 and 10 38 -1.</summary>
    Decimal = 5,
    /// <summary>
    /// <see cref="T:System.Double" />. A floating point number within the range of -1.79E +308 through 1.79E +308.</summary>
    Float = 6,
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. A variable-length stream of binary data ranging from 0 to 2 31 -1 (or 2,147,483,647) bytes.</summary>
    Image = 7,
    /// <summary>
    /// <see cref="T:System.Int32" />. A 32-bit signed integer.</summary>
    Int = 8,
    /// <summary>
    /// <see cref="T:System.Decimal" />. A currency value ranging from -2 63 (or -9,223,372,036,854,775,808) to 2 63 -1 (or +9,223,372,036,854,775,807) with an accuracy to a ten-thousandth of a currency unit.</summary>
    Money = 9,
    /// <summary>
    /// <see cref="T:System.String" />. A fixed-length stream of Unicode characters ranging between 1 and 4,000 characters.</summary>
    NChar = 10, // 0x0000000A
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of Unicode data with a maximum length of 2 30 - 1 (or 1,073,741,823) characters.</summary>
    NText = 11, // 0x0000000B
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of Unicode characters ranging between 1 and 4,000 characters. Implicit conversion fails if the string is greater than 4,000 characters. Explicitly set the object when working with strings longer than 4,000 characters. Use <see cref="F:System.Data.SqlDbType.NVarChar" /> when the database column is <see langword="nvarchar(max)" />.</summary>
    NVarChar = 12, // 0x0000000C
    /// <summary>
    /// <see cref="T:System.Single" />. A floating point number within the range of -3.40E +38 through 3.40E +38.</summary>
    Real = 13, // 0x0000000D
    /// <summary>
    /// <see cref="T:System.Guid" />. A globally unique identifier (or GUID).</summary>
    UniqueIdentifier = 14, // 0x0000000E
    /// <summary>
    /// <see cref="T:System.DateTime" />. Date and time data ranging in value from January 1, 1900 to June 6, 2079 to an accuracy of one minute.</summary>
    SmallDateTime = 15, // 0x0000000F
    /// <summary>
    /// <see cref="T:System.Int16" />. A 16-bit signed integer.</summary>
    SmallInt = 16, // 0x00000010
    /// <summary>
    /// <see cref="T:System.Decimal" />. A currency value ranging from -214,748.3648 to +214,748.3647 with an accuracy to a ten-thousandth of a currency unit.</summary>
    SmallMoney = 17, // 0x00000011
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of non-Unicode data with a maximum length of 2 31 -1 (or 2,147,483,647) characters.</summary>
    Text = 18, // 0x00000012
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. Automatically generated binary numbers, which are guaranteed to be unique within a database. <see langword="timestamp" /> is used typically as a mechanism for version-stamping table rows. The storage size is 8 bytes.</summary>
    Timestamp = 19, // 0x00000013
    /// <summary>
    /// <see cref="T:System.Byte" />. An 8-bit unsigned integer.</summary>
    TinyInt = 20, // 0x00000014
    /// <summary>
    /// <see cref="T:System.Array" /> of type <see cref="T:System.Byte" />. A variable-length stream of binary data ranging between 1 and 8,000 bytes. Implicit conversion fails if the byte array is greater than 8,000 bytes. Explicitly set the object when working with byte arrays larger than 8,000 bytes.</summary>
    VarBinary = 21, // 0x00000015
    /// <summary>
    /// <see cref="T:System.String" />. A variable-length stream of non-Unicode characters ranging between 1 and 8,000 characters. Use <see cref="F:System.Data.SqlDbType.VarChar" /> when the database column is <see langword="varchar(max)" />.</summary>
    VarChar = 22, // 0x00000016
    /// <summary>
    /// <see cref="T:System.Object" />. A special data type that can contain numeric, string, binary, or date data as well as the SQL Server values Empty and Null, which is assumed if no other type is declared.</summary>
    Variant = 23, // 0x00000017
    /// <summary>An XML value. Obtain the XML as a string using the <see cref="M:System.Data.SqlClient.SqlDataReader.GetValue(System.Int32)" /> method or <see cref="P:System.Data.SqlTypes.SqlXml.Value" /> property, or as an <see cref="T:System.Xml.XmlReader" /> by calling the <see cref="M:System.Data.SqlTypes.SqlXml.CreateReader" /> method.</summary>
    Xml = 25, // 0x00000019
    /// <summary>A SQL Server user-defined type (UDT).</summary>
    Udt = 29, // 0x0000001D
    /// <summary>A special data type for specifying structured data contained in table-valued parameters.</summary>
    Structured = 30, // 0x0000001E
    /// <summary>Date data ranging in value from January 1,1 AD through December 31, 9999 AD.</summary>
    Date = 31, // 0x0000001F
    /// <summary>Time data based on a 24-hour clock. Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds. Corresponds to a SQL Server <see langword="time" /> value.</summary>
    Time = 32, // 0x00000020
    /// <summary>Date and time data. Date value range is from January 1,1 AD through December 31, 9999 AD. Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds.</summary>
    DateTime2 = 33, // 0x00000021
    /// <summary>Date and time data with time zone awareness. Date value range is from January 1,1 AD through December 31, 9999 AD. Time value range is 00:00:00 through 23:59:59.9999999 with an accuracy of 100 nanoseconds. Time zone value range is -14:00 through +14:00.</summary>
    DateTimeOffset = 34, // 0x00000022,
    CsIdentifier,
    Unknown,
    Null,
    Max,
    ArgMixed,
    HierarchyId,
    Geometry,
    Geography,
    Sysname
}