using Minfold.SqlJson;

namespace Minfold.SqlJsonCli;

class Program
{
    static async Task Main(string[] args)
    {
        SqlPocoGeneratorTool.SqlModelGenerator x = new SqlPocoGeneratorTool.SqlModelGenerator("Data Source=zubejda2;Initial Catalog=Priprava;User ID=mstagl-prg;Password=123123123Aa$;TrustServerCertificate=True;Encrypt=False");
        
        string cls = await x.GenerateModelClassesAsync($$$"""
                                                     select (
                                                         select u.*, u.test2 as test3
                                                         for json path
                                                     )
                                                     from (
                                                         select *
                                                         from (
                                                             select 1 as col1, (
                                                                 select 4 + (
                                                                     select 2 + 2
                                                                 )    
                                                             ) as n, 'test' as col2, (
                                                                 select top 1 u.IDuser
                                                                 from [User] u
                                                                 for json path
                                                             ) as test2
                                                         ) as iq
                                                     ) as u
                                                     join LlmJobs as test on 1 = 1
                                                     """);
        
        /*string cls = await x.GenerateModelClasses($$$"""
                                  select j.userId, count(j.id) as preps, (
                                      select u.FirstName + ' ' + u.LastName as name, u.EmailAddress, u.DateRegistered, u.SchoolDateJoined, j.userId
                                      from [User] u
                                      where u.IDuser = j.userId
                                      for json path, without_array_wrapper
                                  ) as userInfo
                                  from LlmJobs j
                                  where j.schoolId = 1
                                  group by j.userId
                                  """);*/

        Console.WriteLine(cls);

        //await MinfoldSql.Map();
    }
}