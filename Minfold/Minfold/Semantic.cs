
using Microsoft.Build.Locator;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.MSBuild;
using Microsoft.CodeAnalysis.Rename;

namespace Minfold;

public static class Semantic
{
    private static bool msBuildRegistered;
    
    public static void RegisterMsBuild()
    {
        IEnumerable<VisualStudioInstance> instances = MSBuildLocator.QueryVisualStudioInstances().OrderByDescending(instance => instance.Version);
        MSBuildLocator.RegisterInstance(instances.FirstOrDefault());   
    }
    
    public static async Task<SemanticSolution> CreateWorkspace(CsSource source)
    {
        if (!msBuildRegistered)
        {
            RegisterMsBuild();
        }
        
        MSBuildWorkspace msWorkspace = MSBuildWorkspace.Create();
        Solution solution = await msWorkspace.OpenSolutionAsync(source.ProjectPath);

        return new SemanticSolution(msWorkspace, solution);
    }

    public static async Task RenameSymbol(SemanticSolution solution)
    {
        /*Project? prj = solution.Solution.Projects.FirstOrDefault();
        Document? userDao = prj.Documents.FirstOrDefault(x => x.Name is "UserDao.cs");
        SemanticModel? model22 = await userDao.GetSemanticModelAsync();
        INamedTypeSymbol? symbol = model22.Compilation.GetTypeByMetadataName("ScioSkoly.Chat.User");
        SyntaxNode root = await model22.SyntaxTree.GetRootAsync();
        
        Solution newSol = await Renamer.RenameSymbolAsync(solution, symbol, new SymbolRenameOptions(RenameFile: true), "HihiHaha");

        prj = newSol.Projects.FirstOrDefault();
        userDao = prj.Documents.FirstOrDefault(x => x.Name is "UserDao.cs");
        model22 = await userDao.GetSemanticModelAsync();
        symbol = model22.Compilation.GetTypeByMetadataName("ScioSkoly.Chat.User");
        root = await model22.SyntaxTree.GetRootAsync();

        string text = root.NormalizeWhitespace().ToFullString();
        
        int z = 0;*/
    }
}

