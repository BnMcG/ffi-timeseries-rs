using System.Runtime.InteropServices;

class Program
{
    [DllImport("libtimeseries_rs", CallingConvention = CallingConvention.Cdecl)]
    public static extern IntPtr query_timeseries(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string query,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string timeseries_table_uri_c
    );

    static void Main()
    {
        Console.WriteLine($"{DateTime.Now} Querying Delta Lake via FFI...");

        string query = "SELECT * FROM timeseries LIMIT 1";
        string uri = "/home/ben/ffi-timeseries-rs/timeseries";

        Console.WriteLine($"Query: {query}");
        Console.WriteLine($"URI: {uri}");

        IntPtr arrowReturnPtr = query_timeseries(query, uri);
    }
}
