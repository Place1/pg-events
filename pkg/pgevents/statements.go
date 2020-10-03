package pgevents

import "fmt"

func procedure() string {
	return `
		CREATE OR REPLACE FUNCTION pgevents_notify_event() RETURNS TRIGGER AS $$

		DECLARE
				data json;
				notification json;

		BEGIN

				-- Convert the old or new row to JSON, based on the kind of action.
				-- Action = DELETE?             -> OLD row
				-- Action = INSERT or UPDATE?   -> NEW row
				IF (TG_OP = 'DELETE') THEN
						data = row_to_json(OLD);
				ELSE
						data = row_to_json(NEW);
				END IF;

				-- Contruct the notification as a JSON string.
				notification = json_build_object(
													'table', TG_TABLE_NAME,
													'action', TG_OP,
													'data', data::text);


				-- Execute pg_notify(channel, notification)
				PERFORM pg_notify('pgevents_event', notification::text);

				-- Result is ignored since this is an AFTER trigger
				RETURN NULL;
		END;

		$$ LANGUAGE plpgsql;
	`
}

func trigger(table string) string {
	command := `
			BEGIN;

			DROP TRIGGER IF EXISTS %s ON %s;

			CREATE TRIGGER %s
			AFTER INSERT OR UPDATE OR DELETE ON %s
			FOR EACH ROW EXECUTE PROCEDURE pgevents_notify_event();

			COMMIT;
		`
	name := fmt.Sprintf("%s_events", table)
	return fmt.Sprintf(command, name, table, name, table)
}
