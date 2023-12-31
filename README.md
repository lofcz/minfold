# Minfold
Strongly opinionated EF Core, database-first Scaffolder. Minfold implements the following ideas:

1. Avoids [N+1 problem](https://learn.microsoft.com/en-us/ef/core/performance/efficient-querying) by scaffolding foreign keys as attributes, not types:
 ```diff
 + [ReferenceKey(typeof(T), nameof(T.ColumnName), true]
 + public int TEntity { get; set; }
 - public Collection<T> TEntities { get; set; }
 ```
2. Respects columns mapped to enums as long as types are compatible:
 ```cs
 // When scaffolding for the first time the column is scaffolded as int
 // once the type is replaced with a compatible identifier, Minfold respects the new type
 public MyEnum MyColumn { get; set; }
 ```
3. Generates DAO classes for each model, auto-detects if basic methods such as `GetWhereId()` need to be scaffolded or implemented by a common interface and in the former case scaffolds them.
4. Operates without `SemanticModel` by default with an optional elevation. This enables scaffolding even when the project cannot be built due to errors and enables blazingly fast scaffolding times, typically under 1s for 100 tables.
5. Can be configured to scaffold commonly named columns to special values, and omitting/defaulting such values in constructors. For example, a column named `dateCreated` will always be scaffolded as `DateTime.Now`.
6. Produces terse `DbSet<T>` mapping if columns are convention-named.

## Getting Started

### Rider

Download the plugin from [JetBrains Marketplace](https://plugins.jetbrains.com/plugin/23315-minfold?noRedirect=true) or from [here](https://github.com/lofcz/minfold/tree/master/Plugins/Rider/dist).

### Visual Studio

Download the plugin from [Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=lofcz.minfold) or from [here](https://github.com/lofcz/minfold/tree/master/Plugins/Vs/dist).

### CLI

```bash
dotnet tool install Minfold.Cli --global
```

To invoke:
```
minfold --help
```

Usage:
```
minfold --database DATABASE --connection "CONNECTION_STRING" --codePath "C:\.."
```

## Limitations

Currently, Manfold doesn't generate migrations. This is being worked on. Minfold doesn't support views and stored procedures, this is not planned.
