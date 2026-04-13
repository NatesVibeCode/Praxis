import { useState, useEffect, useCallback } from 'react';

export interface ObjectType {
  type_id: string;
  name: string;
  description: string;
  icon: string;
  property_definitions: any[];
}

export function useObjectTypes(): { objectTypes: ObjectType[]; loading: boolean } {
  const [objectTypes, setObjectTypes] = useState<ObjectType[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchObjectTypes = useCallback(async () => {
    try {
      const res = await fetch('/api/object-types').then((r) => (r.ok ? r.json() : null)).catch(() => null);
      setObjectTypes((res?.types ?? []) as ObjectType[]);
    } catch {
      // silent
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchObjectTypes();
  }, [fetchObjectTypes]);

  return { objectTypes, loading };
}
