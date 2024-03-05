using System.Globalization;
using System.Text.RegularExpressions;
namespace Minfold;

/*
 * Based on the original Inflector .NET: https://github.com/srkirkland/Inflector
 * Refined with Humanizr cases: https://github.com/Humanizr/Humanizer/blob/main/src/Humanizer/Inflections/Vocabularies.cs
 * Both libraries licensed under MIT
 *
 * Further improved by Minfold Contributors
 */

public static class Inflector
{
    public static readonly List<string> Suffixes = ["i", "es", "oes", "a", "ses", "ves", "ies", "ices", "ice", "en", "zes", "ae", "um"];
    private static readonly List<Rule> plurals = [];
    private static readonly List<Rule> singulars = [];
    private static readonly HashSet<string> uncountables = [];
    
    public static string? Plural(this string str)
    {
        Tuple<string, string> parts = str.SplitPascal();
        string? result = ApplyRules(plurals, parts.Item2);
        return result is not null ? $"{parts.Item1}{result}" : null;
    }

    public static string? Singular(this string str)
    {
        Tuple<string, string> parts = str.SplitPascal();
        string? result = ApplyRules(singulars, parts.Item2);
        return result is not null ? $"{parts.Item1}{result}" : null;
    }
    
    static Inflector()
    {
        AddPlural("$", "s");
        AddPlural("s$", "s");
        AddPlural("(ax|test)is$", "$1es");
        AddPlural("(octop|vir|alumn|fung|cact|foc|hippopotam|radi|stimul|syllab|nucle)us$", "$1i");
        AddPlural("(alias|bias|iris|status|campus|apparatus|virus|walrus|trellis)$", "$1es");
        AddPlural("(bu)s$", "$1ses");
        AddPlural("(buffal|tomat|volcan|ech|embarg|her|mosquit|potat|torped|vet)o$", "$1oes");
        AddPlural("([dti])um$", "$1a");
        AddPlural("sis$", "ses");
        AddPlural("(?:([^f])fe|([lr])f)$", "$1$2ves");
        AddPlural("(hive)$", "$1s");
        AddPlural("([^aeiouy]|qu)y$", "$1ies");
        AddPlural("(x|ch|ss|sh)$", "$1es");
        AddPlural("(matr|vert|ind)(ix|ex)$", "$1ices");
        AddPlural("([m|l])ouse$", "$1ice");
        AddPlural("^(ox)$", "$1en");
        AddPlural("(quiz)$", "$1zes");
        AddPlural("(buz|blit|walt)z$", "$1zes");
        AddPlural("(hoo|lea|loa|thie)f$", "$1ves");
        AddPlural("(alumn|alg|larv|vertebr)a$", "$1ae");
        AddPlural("(criteri|phenomen)on$", "$1a");
        
        AddSingular("s$", "");
        AddSingular("(n)ews$", "$1ews");
        AddSingular("([dti])a$", "$1um");
        AddSingular("(analy|ba|diagno|parenthe|progno|synop|the|ellip|empha|neuro|oa|paraly)ses", "$1sis");
        AddSingular("([^f])ves$", "$1fe");
        AddSingular("(hive)s$", "$1");
        AddSingular("(tive)s$", "$1");
        AddSingular("([lr]|hoo|lea|loa|thie)ves$", "$1f");
        AddSingular("(^zomb)?([^aeiouy]|qu)ies$", "$2y");
        AddSingular("(s)eries$", "$1eries");
        AddSingular("(m)ovies$", "$1ovie");
        AddSingular("(x|ch|ss|sh)es$", "$1");
        AddSingular("([m|l])ice$", "$1ouse");
        AddSingular("(?<!^[a-z])(o)es$", "$1");
        AddSingular("(shoe)s$", "$1");
        AddSingular("(cris|ax|test)es$", "$1is");
        AddSingular("(octop|vir|alumn|fung|cact|foc|hippopotam|radi|stimul|syllab|nucle)i$", "$1us");
        AddSingular("(alias|bias|iris|status|campus|apparatus|virus|walrus|trellis)es$", "$1");
        AddSingular("^(ox)en", "$1");
        AddSingular("(matr|d)ices$", "$1ix");
        AddSingular("(vert|ind)ices$", "$1ex");
        AddSingular("(quiz)zes$", "$1");
        AddSingular("(buz|blit|walt)zes$", "$1z");
        AddSingular("(alumn|alg|larv|vertebr)ae$", "$1a");
        AddSingular("(criteri|phenomen)a$", "$1on");
        AddSingular("([b|r|c]ook|room|smooth)ies$", "$1ie");

        AddIrregular("person", "people");
        AddIrregular("man", "men");
        AddIrregular("human", "humans");
        AddIrregular("child", "children");
        AddIrregular("sex", "sexes");
        AddIrregular("glove", "gloves");
        AddIrregular("move", "moves");
        AddIrregular("goose", "geese");
        AddIrregular("wave", "waves");
        AddIrregular("foot", "feet");
        AddIrregular("tooth", "teeth");
        AddIrregular("curriculum", "curricula");
        AddIrregular("database", "databases");
        AddIrregular("zombie", "zombies");
        AddIrregular("personnel", "personnel");
        AddIrregular("cache", "caches");
        AddIrregular("child", "children");
        AddIrregular("ex", "exes", false);
        AddIrregular("is", "are", false);
        AddIrregular("that", "those", false);
        AddIrregular("this", "these", false);
        AddIrregular("bus", "buses", false);
        AddIrregular("die", "dice", false);
        AddIrregular("tie", "ties", false);

        List<string> uncountablesWords =
        [
            "accommodation","advertising","air","aid","advice","anger","art","assistance","bread","business","butter","calm","cash","chaos","cheese","childhood",
            "clothing","coffee","content","corruption","courage","currency","damage","danger","darkness","data","determination","economics","education","electricity",
            "employment","energy","entertainment","enthusiasm","equipment","evidence","failure","fame","fire","flour","food","freedom","friendship","fuel","furniture",
            "fun","genetics","gold","grammar","guilt","hair","happiness","harm","health","heat","help","homework","honesty","hospitality","housework","humour",
            "imagination","importance","information","innocence","intelligence","jealousy","juice","justice","kindness","knowledge","labour","lack","laughter","leisure","literature",
            "litter","logic","love","luck","magic","management","metal","milk","money","motherhood","motivation","music","nature","news","nutrition","obesity","oil",
            "old age","oxygen","paper","patience","permission","pollution","poverty","power","pride","production","progress","pronunciation","publicity","punctuation","quality",
            "quantity","racism","rain","relaxation","research","respect","rice","room","rubbish","safety","salt","sand","seafood","shopping","silence","smoke",
            "snow","software","soup","speed","spelling","stress","sugar","sunshine", "staff", "training", "corn", "metadata","mail","means","scissors","corps","tuna","trout",
            "swine","someone","shrimp","salmon","offspring","moose","luggage","elk","mud","grass","bison","sperm","semen","waters","water","aircraft","oz","tsp","tbsp",
            "sheep","fish","series","species","christmas","aggression","attention","bacon","baggage","ballet","beauty","beef","beer","biology","blood","botany","carbon","cardboard",
            "chalk","chess","coal","commerce","compassion","comprehension","cotton","dancing","delight","dessert","dignity","dirt","distribution","dust","engineering",
            "enjoyment","envy","ethics","evolution","faith","fiction","flu","fruit","garbage","garlic","gas","glass","golf","gossip","gratitude","grief","ground","gymnastics",
            "hardware","hate","hatred","height","honey","hunger","hydrogen","ice","ice cream","inflation","injustice","iron","irony","jam","jelly","joy","judo","karate","land",
            "lava","leather","lightning","linguistics","livestock","loneliness","machinery","mankind","marble","mathematics","mayonnaise","measles","meat","methane","nitrogen",
            "nonsense","nurture","obedience","passion","pasta","physics","poetry","psychology","quartz","recreation","reliability","revenge","rum","salad","satire","scenery",
            "seaside","shame","sleep","smoking","soap","soil","sorrow","sport","steam","strength","stuff","stupidity","success","symmetry","tea","tennis","thirst","thunder",
            "timber","time","toast","tolerance","trade","traffic","transportation","travel","trust","understanding","underwear","unemployment","unity","usage","validity","veal",
            "vegetation","vegetarianism","vengeance","violence","vision","vitality","warmth","wealth","weather","weight","welfare","wheat","whiskey","width","wildlife","wine",
            "wisdom","wood","wool","work","yeast","yoga","youth","zinc","zoology"
        ];  

        foreach (string str in uncountablesWords)
        {
            AddUncountable(str);
        }
    }

    /// <summary>
    /// Splits input string into two parts - prefix and the last word, expects PascalCaseString
    /// </summary>
    /// <param name="input"></param>
    /// <returns></returns>
    private static Tuple<string, string> SplitPascal(this string input)
    {
        bool firstUpperFound = false;
        int splitIndex = input.Length;
        
        for (int i = input.Length - 1; i > 0; i--)
        {
            if (char.IsUpper(input[i]))
            {
                if (!firstUpperFound)
                {
                    firstUpperFound = true;
                }
            }
            else if (firstUpperFound)
            {
                break;
            }

            splitIndex--;
        }

        return !firstUpperFound ? new Tuple<string, string>(string.Empty, input) : new Tuple<string, string>(input[..splitIndex], input[splitIndex..]);
    }

    private class Rule(string pattern, string replacement)
    {
        private readonly Regex regex = new(pattern, RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public string? Apply(string word)
        {
            return !regex.IsMatch(word) ? null : regex.Replace(word, replacement);
        }
    }

    private static void AddIrregular(string singular, string plural, bool matchEnding = true)
    {
        if (matchEnding)
        {
            AddPlural($"({singular[0]}){singular[1..]}$", $"$1{plural[1..]}");
            AddSingular($"({plural[0]}){plural[1..]}$", $"$1{singular[1..]}");
        }
        else
        {
            AddPlural($"^{singular}$", plural);
            AddSingular($"^{plural}$", singular);
        }
    }

    private static void AddUncountable(string word)
    {
        uncountables.Add(word.ToLower());
    }

    private static void AddPlural(string rule, string replacement)
    {
        plurals.Add(new Rule(rule, replacement));
    }

    private static void AddSingular(string rule, string replacement)
    {
        singulars.Add(new Rule(rule, replacement));
    }
    
    private static string? ApplyRules(IReadOnlyList<Rule> rules, string word)
    {
        string? result = word;

        if (!uncountables.Contains(word.ToLowerInvariant()))
        {
            for (int i = rules.Count - 1; i >= 0; i--)
            {
                if ((result = rules[i].Apply(word)) is not null)
                {
                    break;
                }
            }
        }

        return result;
    }
}