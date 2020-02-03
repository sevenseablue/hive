package com.qunar.hive.jdbc.authority.entry;

import java.lang.annotation.*;

/**
 * Created with Lee. Date: 2019/9/25 Time: 14:41 To change this template use File | Settings | File
 * Templates. Description:
 *
 * @author : hongweis.li
 */

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Table {

  String value() default "";
}
