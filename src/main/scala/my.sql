Select
  t1.time,
  (
    Select
      COUNT(Distinct ip)
    From
      plugin_log
    Where
      `time` < DATE_ADD(t1.time, Interval 1 Day)
  ) As num,
  (
    Select
      COUNT(1) As 'num'
    From
      `plugin_log`
    Where
      `time` Like concat(t1.time, '%')
  ) As PV,
  (
    Select
      COUNT(Distinct ip) As 'num'
    From
      `plugin_log`
    Where
      `time` Like concat(t1.time, '%')
  ) As UV,
  (
    Select
      COUNT(Distinct ip)
    From
      `plugin_log`
    Where
      `time` Like concat(DATE_SUB(t1.time, Interval 1 Day), '%')
  ) - (
        Select
          COUNT(Distinct ip)
        From
          `plugin_log`
        Where
          `time` Like concat(t1.time, '%')
      )
From
  (
    Select
      SUBSTR(Time, 1, 10) As time
    From
      `plugin_log`
    Where
        `time` < '2021-09-08'
    And `time` >= '2021-09-01'
    Group By
      SUBSTR(Time, 1, 10)
  ) t1