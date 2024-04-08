package com.puchen.java.flink117.p0dome;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilesourceDome {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> build = FileSource
                    .forRecordStreamFormat(
                                new TextLineInputFormat()
                            , new Path("D:\\chenpu\\tiem3\\hive_udf-master\\src\\main\\java\\com\\craiditx\\flink\\env\\test.txt")
                    )
                .build();
        env.fromSource(build,WatermarkStrategy.noWatermarks(),"source_file").print();

        env.execute();
    }
}
