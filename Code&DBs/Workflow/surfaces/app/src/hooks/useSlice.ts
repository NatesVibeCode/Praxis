import { useEffect, useState } from 'react';
import { World } from '../world';

export function useSlice(w: World, path: string | null | undefined): unknown {
  const [value, setValue] = useState(() => w.get(path));

  useEffect(() => {
    setValue(w.get(path));
    return w.subscribe(path, setValue);
  }, [w, path]);

  return value;
}
