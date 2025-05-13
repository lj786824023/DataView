DELIMITER |

CREATE DEFINER="gbase"@"%" FUNCTION "etl_func_get_month_end_date"(in_data_date varchar(8)) RETURNS varchar(8) CHARSET utf8
begin                                                                                                                                                                                                                           
  declare etl_data_date varchar (8) default in_data_date;                                                                                                                                                                                
          /**得到当前日期yyyymmdd的月末日期*/                                                                       
          return to_char((trunc(etl_data_date,'mm')+interval 1 month - interval 1 day),'yyyymmdd');                                                                                    
  end |