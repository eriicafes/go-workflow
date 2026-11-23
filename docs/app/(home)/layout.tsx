import { HomeLayout } from 'fumadocs-ui/layouts/home';
import { baseOptions } from '@/lib/layout.shared';

export default function Layout({ children }: LayoutProps<'/'>) {
  const base = baseOptions();
  return (
    <HomeLayout
      {...base}
      links={[{ text: 'Docs', url: '/docs', active: 'nested-url' }, ...(base.links ?? [])]}
    >
      {children}
    </HomeLayout>
  );
}
