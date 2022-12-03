with cte as (SELECT n.*, 
    CASE when debit_credit = 'credit' 
    then transaction_amount * 1
    else transaction_amount * -1 end as trans_amt
    from public.account_balance n),
final_data as
            (SELECT account_no,
            sum(trans_amt)
            over(partition by account_no order by 
            transaction_date) as current_balance,
            sum(trans_amt)
            over(partition by account_no order by 
            transaction_date range between 
            unbounded preceding and
            unbounded following) as final_balance,
            case when sum(trans_amt) over(
            partition by account_no order by
            transaction_date) >= 1000
            then 1 else 0 end as flag,
            transaction_date from cte
            )
       
            
select account_no, min(transaction_date) as transaction_date
from final_data
where final_balance >= 1000 and flag =1
group by account_no




             