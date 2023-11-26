using System.Text.RegularExpressions;

namespace Minfold;

public static class Regexes
{
    public static readonly Regex MultilineCommentDynamic = new Regex("^\\/\\*[\\s]*dynamic[\\s]*\\*\\/$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Multiline);
    public static readonly Regex SinglelineCommentDynamic = new Regex("^\\/\\/[\\s]*dynamic[\\s]*$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Multiline);
}