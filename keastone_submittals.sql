With date_filter As (
  Select
      Cast(? As Datetime) start_date
   , Cast(? As Datetime) end_date
)

Select
    op.name As case_number
  , op.new_submittaldate As revenue_date
  , op.new_keanerevenue As revenue_amount
  , Case
      When Coalesce(op.new_international, 0) = 1 Then 'International'
      When op.new_producttypename In (
        'Mutual Fund', 'Corporate', 'UIT', 'Pensions'
      ) Then 'Securities'
      When op.new_producttypename In (
        'Broker', 'Post Escheat'
      ) then op.new_producttypename
      Else 'Unknown'
    End As revenue_category
  , op.new_servicename
  , op.new_submittaltypename
From
  FilteredOpportunity as op
  , date_filter df
Where
  op.new_submittaldateutc >= start_date
  And op.new_submittaldateutc < end_date
  And op.new_producttypename != 'Banking' -- banking rev on cash, not submittal

UNION All

Select
    s.name
  , s.new_datecheckreceivedutc
  , Sum(s.new_keanerevenue)
  , 'Banking'
  , 'Deep Search'
  , 'LCS Submittal'
From
  FilteredSalesOrder as s
  Join
  FilteredOpportunity As op On
    s.opportunityid = op.opportunityid
--  And op.new_submittaldateutc Is Not Null
--  And op.new_productname Is Not Null
  Cross Join
  date_filter df
Where
  s.new_datecheckreceivedutc >= start_date
  And s.new_datecheckreceivedutc < end_date
  And s.new_accountingtransaction In (1, 2) -- "Accounting Transaction In ('AP/AR', 'AR')
  And s.new_keanerevenue > 0
  And op.new_producttypename = 'Banking'
Group By
    s.name
  , s.new_datecheckreceivedutc

;