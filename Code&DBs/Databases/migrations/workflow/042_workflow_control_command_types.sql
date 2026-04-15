BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = current_schema()
          AND table_name = 'control_commands'
    ) THEN
        UPDATE control_commands
        SET command_type = CASE command_type
            WHEN 'dispatch.submit' THEN 'workflow.submit'
            WHEN 'dispatch.retry' THEN 'workflow.retry'
            WHEN 'dispatch.cancel' THEN 'workflow.cancel'
            ELSE command_type
        END
        WHERE command_type IN ('dispatch.submit', 'dispatch.retry', 'dispatch.cancel');

        ALTER TABLE control_commands
            DROP CONSTRAINT IF EXISTS control_commands_command_type_check;

        ALTER TABLE control_commands
            ADD CONSTRAINT control_commands_command_type_check
            CHECK (
                command_type IN (
                    'workflow.submit',
                    'workflow.spawn',
                    'workflow.chain.submit',
                    'workflow.retry',
                    'workflow.cancel',
                    'sync.repair'
                )
            );
    END IF;
END
$$;

COMMIT;
