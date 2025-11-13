using System.Collections.Concurrent;
using System.Data;
using Minfold;

namespace MinFold.Test;

[TestFixture]
public class ColumnRebuildDetectionTests
{
    [Test]
    public void TestTextTypeChangeRequiresRebuild()
    {
        // Arrange: Create old column (varchar) and new column (text)
        SqlTableColumn oldColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlTableColumn newColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.Text, [], false, false, null, null);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Converting varchar to text should require rebuild");
    }

    [Test]
    public void TestNTextTypeChangeRequiresRebuild()
    {
        // Arrange: Create old column (nvarchar) and new column (ntext)
        SqlTableColumn oldColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.NVarChar, [], false, false, null, 100);
        SqlTableColumn newColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.NText, [], false, false, null, null);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Converting nvarchar to ntext should require rebuild");
    }

    [Test]
    public void TestImageTypeChangeRequiresRebuild()
    {
        // Arrange: Create old column (varbinary) and new column (image)
        SqlTableColumn oldColumn = new SqlTableColumn("Data", 1, true, false, SqlDbTypeExt.VarBinary, [], false, false, null, 100);
        SqlTableColumn newColumn = new SqlTableColumn("Data", 1, true, false, SqlDbTypeExt.Image, [], false, false, null, null);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Converting varbinary to image should require rebuild");
    }

    [Test]
    public void TestIdentityChangeRequiresRebuild()
    {
        // Arrange: Create old column (not identity) and new column (identity)
        SqlTableColumn oldColumn = new SqlTableColumn("Id", 1, false, false, SqlDbTypeExt.Int, [], false, false, null, null);
        SqlTableColumn newColumn = new SqlTableColumn("Id", 1, false, true, SqlDbTypeExt.Int, [], false, false, null, null, 1, 1);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Changing identity property should require rebuild");
    }

    [Test]
    public void TestComputedChangeRequiresRebuild()
    {
        // Arrange: Create old column (not computed) and new column (computed)
        SqlTableColumn oldColumn = new SqlTableColumn("Total", 1, true, false, SqlDbTypeExt.Int, [], false, false, null, null);
        SqlTableColumn newColumn = new SqlTableColumn("Total", 1, true, false, SqlDbTypeExt.Int, [], true, false, "Price * Quantity", null);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Changing computed property should require rebuild");
    }

    [Test]
    public void TestComputedFormulaChangeRequiresRebuild()
    {
        // Arrange: Create old column (computed) and new column (computed with different formula)
        SqlTableColumn oldColumn = new SqlTableColumn("Total", 1, true, false, SqlDbTypeExt.Int, [], true, false, "Price * Quantity", null);
        SqlTableColumn newColumn = new SqlTableColumn("Total", 1, true, false, SqlDbTypeExt.Int, [], true, false, "Price * Quantity * Tax", null);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Changing computed formula should require rebuild");
    }

    [Test]
    public void TestTimestampColumnChangeRequiresRebuild()
    {
        // Arrange: Create old column (timestamp) and new column (timestamp with different properties)
        SqlTableColumn oldColumn = new SqlTableColumn("Version", 1, false, false, SqlDbTypeExt.Timestamp, [], false, false, null, null);
        SqlTableColumn newColumn = new SqlTableColumn("Version", 1, true, false, SqlDbTypeExt.Timestamp, [], false, false, null, null);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Changing timestamp column should require rebuild");
    }

    [Test]
    public void TestPositionChangeWithIndexDependenciesRequiresRebuild()
    {
        // Arrange: Create old column and new column with position change, column has index dependency
        SqlTableColumn oldColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlTableColumn newColumn = new SqlTableColumn("Name", 2, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlIndex index = new SqlIndex("IX_Name", "TestTable", ["Name"], false, "dbo");
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn> { ["name"] = oldColumn }, [index], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Position change with index dependencies should require rebuild");
    }

    [Test]
    public void TestPositionChangeWithComputedDependenciesRequiresRebuild()
    {
        // Arrange: Create old column and new column with position change, another column depends on it
        SqlTableColumn oldColumn = new SqlTableColumn("Price", 1, true, false, SqlDbTypeExt.Decimal, [], false, false, null, 18);
        SqlTableColumn newColumn = new SqlTableColumn("Price", 2, true, false, SqlDbTypeExt.Decimal, [], false, false, null, 18);
        SqlTableColumn computedColumn = new SqlTableColumn("Total", 2, true, false, SqlDbTypeExt.Decimal, [], true, false, "[Price] * [Quantity]", null);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn> 
        { 
            ["price"] = oldColumn,
            ["total"] = computedColumn
        }, [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns true
        Assert.That(requiresRebuild, Is.True, "Position change with computed dependencies should require rebuild");
    }

    [Test]
    public void TestSimpleNullableChangeDoesNotRequireRebuild()
    {
        // Arrange: Create old column (nullable) and new column (not nullable)
        SqlTableColumn oldColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlTableColumn newColumn = new SqlTableColumn("Name", 1, false, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns false
        Assert.That(requiresRebuild, Is.False, "Simple nullable change should not require rebuild");
    }

    [Test]
    public void TestLengthChangeDoesNotRequireRebuild()
    {
        // Arrange: Create old column (varchar(100)) and new column (varchar(200))
        SqlTableColumn oldColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlTableColumn newColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 200);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns false
        Assert.That(requiresRebuild, Is.False, "Length change should not require rebuild");
    }

    [Test]
    public void TestDefaultConstraintChangeDoesNotRequireRebuild()
    {
        // Arrange: Create old column (no default) and new column (with default)
        SqlTableColumn oldColumn = new SqlTableColumn("Status", 1, true, false, SqlDbTypeExt.Int, [], false, false, null, null);
        SqlTableColumn newColumn = new SqlTableColumn("Status", 1, true, false, SqlDbTypeExt.Int, [], false, false, null, null, null, null, "DF_Status", "0");
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns false
        Assert.That(requiresRebuild, Is.False, "Default constraint change should not require rebuild");
    }

    [Test]
    public void TestPositionChangeWithoutDependenciesDoesNotRequireRebuild()
    {
        // Arrange: Create old column and new column with position change, no dependencies
        SqlTableColumn oldColumn = new SqlTableColumn("Name", 1, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlTableColumn newColumn = new SqlTableColumn("Name", 2, true, false, SqlDbTypeExt.VarChar, [], false, false, null, 100);
        SqlTable table = new SqlTable("TestTable", new Dictionary<string, SqlTableColumn>(), [], "dbo");

        // Act: Check RequiresRebuild
        bool requiresRebuild = ColumnRebuildDetector.RequiresRebuild(oldColumn, newColumn, table);

        // Assert: Returns false (position changes without dependencies don't require rebuild in our implementation)
        // Note: This is a design decision - we only require rebuild for position changes with dependencies
        Assert.That(requiresRebuild, Is.False, "Position change without dependencies should not require rebuild");
    }
}

