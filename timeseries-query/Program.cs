using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.C;
using Apache.Arrow.Types;
using Spectre.Console;
using Table = Spectre.Console.Table;

// Must match the FfiReturnValue struct in Rust
[StructLayout(LayoutKind.Sequential)]
struct FfiReturnValue
{
    public CArrowArray array;
    public CArrowSchema schema;
}

class Program
{
    [DllImport("libtimeseries_rs", CallingConvention = CallingConvention.Cdecl)]
    public static extern FfiReturnValue query_timeseries(
        [MarshalAs(UnmanagedType.LPUTF8Str)] string query,
        [MarshalAs(UnmanagedType.LPUTF8Str)] string timeseries_table_uri_c
    );

    static void Main(string[] args)
    {
        string query = args[0];
        string uri = args[1];

        FfiReturnValue arrowReturnVal = query_timeseries(query, uri);

        // Suspect there's a way to do this without having to create a pointer to 
        // a struct whose value we already have. Just not sure how - 
        // unless in Rust we create a struct which contains a pointer to FFI_ArrowArray,
        // rather than FFI_ArrowArray (and schema) itself?
        unsafe
        {

            CArrowSchema* schemaPtr = &arrowReturnVal.schema;
            var schema = CArrowSchemaImporter.ImportSchema(schemaPtr);

            // Create a table for output to the CLI
            var outputTable = new Table()
                .Border(TableBorder.Square)
                .BorderColor(Color.NavajoWhite1);

            foreach (var field in schema.FieldsList)
            {
                outputTable = outputTable.AddColumn(new TableColumn($"[u][yellow]{field.Name}[/][/] [red]({field.DataType.Name})[/]"));
            }


            CArrowArray* arrayPtr = &arrowReturnVal.array;
            var structType = new StructType(schema.FieldsList);

            // Cast rather than as so we blow up immediately if this isn't a struct array for some reason...
            var array = (StructArray)CArrowArrayImporter.ImportArray(arrayPtr, structType);

            for (int i = 0; i < array.Length; i++)
            {
                var rowValues = new List<string>();

                foreach (var field in array.Fields)
                {
                    rowValues.Add(PrintArrayValue(field, i));
                }

                outputTable = outputTable.AddRow(rowValues.ToArray());
            }

            AnsiConsole.Write(outputTable);
        }
    }

    private static string PrintArrayValue(IArrowArray array, int index) => array switch
    {
        Int32Array int32Array when !int32Array.IsNull(index) => int32Array.GetValue(index).ToString(),
        Int16Array int16Array when !int16Array.IsNull(index) => int16Array.GetValue(index).ToString(),
        FloatArray floatArray when !floatArray.IsNull(index) => floatArray.GetValue(index).ToString(),
        TimestampArray timestampArray when !timestampArray.IsNull(index) => timestampArray.GetTimestamp(index).ToString(),
        // Add cases for other types as needed
        _ => "null"
    } ?? "null";
}
