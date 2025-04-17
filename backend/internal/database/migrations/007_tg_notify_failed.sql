CREATE IF NOT EXISTS TRIGGER tx_failed_update_notify
AFTER UPDATE ON public.transaction_history
FOR EACH ROW
WHEN (
     NEW.status = 'failed'      -- new row is failed
 AND OLD.status IS DISTINCT FROM 'failed'  -- previous row was NOT failed
)
EXECUTE FUNCTION notify_transaction_failed();