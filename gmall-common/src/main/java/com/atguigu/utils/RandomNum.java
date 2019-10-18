package com.atguigu.utils;

import java.util.Random;

/**
 * @author shkstart
 * @create 2019-10-18 15:04
 */
public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }

}
