using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Minfold;

public static class CtorAnalyzer
{
    public static List<string> GetParameterNames(ConstructorDeclarationSyntax node)
    {
        List<string> ctorParams = node.ParameterList.Parameters.Select(param => param.Identifier.ValueText.ToLowerInvariant()).ToList();
        return ctorParams;
    }
}