select *
 from {{ stitch_base_table(var('events_table')) }}
 where livemode = true
