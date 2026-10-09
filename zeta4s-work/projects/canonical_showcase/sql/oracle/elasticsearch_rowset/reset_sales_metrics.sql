begin
  execute immediate 'drop table es_sales_metrics purge';
exception
  when others then
    if sqlcode != -942 then
      raise;
    end if;
end;
