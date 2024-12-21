public class realtime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String>streamSource
                =env.socketTextStream("hadoop102",  9999);
        streamSource.print();
        env.execute();
    }
}
