package org.apache.flink;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Utils {

    public static SourceFunction<String> getSource(){

        return new RandomSource();
    }


    public static class RandomSource implements SourceFunction<String>{

        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            while(true){
                ctx.collect(RandomStringUtils.randomAlphabetic(1));
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {

        }
    }


}
