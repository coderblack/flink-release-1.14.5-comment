package org.apache.flink;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.commons.lang3.RandomUtils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Utils {

    public static SourceFunction<String> getSource(){

        return new RandomSource();
    }


    public static class RandomSource implements SourceFunction<String>{
        String[] eles = {"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
                "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"};
        @Override
        public void run(SourceContext<String> ctx) throws Exception {

            while(true){
                ctx.collect(eles[RandomUtils.nextInt(0,eles.length)]);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {

        }
    }


}
