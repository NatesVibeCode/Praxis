DELETE FROM public.task_type_route_eligibility
WHERE provider_slug = 'anthropic'
  AND task_type IS NULL
  AND model_slug IS NULL
  AND eligibility_status = 'rejected'
  AND reason_code = 'provider_disabled';
