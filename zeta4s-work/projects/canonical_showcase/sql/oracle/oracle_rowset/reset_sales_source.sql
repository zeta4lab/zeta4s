begin
  execute immediate 'drop table sales purge';
exception
  when others then
    if sqlcode != -942 then
      raise;
    end if;
end;
