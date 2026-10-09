begin
  execute immediate 'drop table sales_metrics purge';
exception
  when others then
    if sqlcode != -942 then
      raise;
    end if;
end;
