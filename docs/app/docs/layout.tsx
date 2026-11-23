import { source } from '@/lib/source';
import { DocsLayout } from 'fumadocs-ui/layouts/notebook';
import { baseOptions } from '@/lib/layout.shared';
import { GopherCredit } from '@/components/gopher-credit';

export default function Layout({ children }: LayoutProps<'/docs'>) {
  const { nav, ...base } = baseOptions();
  return (
    <DocsLayout
      tree={source.getPageTree()}
      tabMode="navbar"
      nav={{ ...nav, mode: 'top' }}
      sidebar={{ footer: <GopherCredit className="px-2 pt-2 text-xs text-fd-muted-foreground" /> }}
      {...base}
    >
      {children}
    </DocsLayout>
  );
}
