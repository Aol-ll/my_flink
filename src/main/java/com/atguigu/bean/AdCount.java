package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Aol
 * @create 2021-01-30 13:42
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdCount {
    private String province;
    private Long windowEnd;
    private Integer count;
}
