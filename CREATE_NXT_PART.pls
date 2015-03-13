CREATE OR REPLACE PROCEDURE CREATE_NXT_PART AS 


v_table_name VARCHAR2(30) := 'WLI_QS_REPORT_ATTRIBUTE';
v_partition_prefix VARCHAR(20) := 'P_WLI_QS_REPORT_';
v_max_part VARCHAR2(30) := v_partition_prefix||'MAX';
v_last_part VARCHAR2(30);
v_next_part VARCHAR2(30);
v_create_next_part VARCHAR2(300);
v_parallelism NUMBER := 1;

cursor cur_last_part is (
select max(partition_name) last_partition 
from user_tab_partitions 
where 
table_name=v_table_name and 
partition_name != v_max_part);

BEGIN
  dbms_output.enable (1000000);
  -- Get next month partition name
  execute immediate 'alter session set nls_date_format=YYYYMM';
  v_next_part := v_partition_prefix||to_char(last_day(sysdate)+1);
  dbms_output.put_line('Next month partition would be : '||v_next_part);
  
  -- Get last existing partition
  open cur_last_part;
  fetch cur_last_part into v_last_part;
  close cur_last_part;
  dbms_output.put_line('Last existing partition is : '||v_last_part);
  
  -- If last partition is greater than next month partition, do nothing. Else split max partition.
  if v_last_part >= v_next_part then
    dbms_output.put_line('Next month partition is less than or equal to last existing partition, doing nothing.');
  else
    dbms_output.put_line('Creating Next Month Partition : '||v_next_part);
    
    v_create_next_part := 'alter table '||v_table_name||' split partition '
    ||v_max_part||' at (to_date('||to_char(last_day(add_months(sysdate,1))+1)||')'
    ||') into (partition '||v_next_part||', partition '||v_max_part||') update global indexes parallel '||v_parallelism;
        
    execute immediate v_create_next_part;
    
  end if;
   
   
END CREATE_NXT_PART;
