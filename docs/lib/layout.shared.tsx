import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { Logo } from '@/components/logo';
import { appName, gitConfig } from './shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <>
          <Logo className="h-7 w-auto" />
          <span className="font-mono text-[15px] font-medium tracking-tight">{appName}</span>
        </>
      ),
    },
    links: [
      {
        text: 'pkg.go.dev',
        url: 'https://pkg.go.dev/github.com/eriicafes/go-workflow',
        external: true,
      },
    ],
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
