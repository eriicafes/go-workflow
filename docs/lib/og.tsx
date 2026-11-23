import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { ImageResponse } from 'next/og';
import { appName } from './shared';

export const ogSize = { width: 1200, height: 628 };

const root = process.cwd();

async function loadAssets() {
  const [regular, semibold, logo] = await Promise.all([
    readFile(path.join(root, 'node_modules/geist/dist/fonts/geist-sans/Geist-Regular.ttf')),
    readFile(path.join(root, 'node_modules/geist/dist/fonts/geist-sans/Geist-SemiBold.ttf')),
    readFile(path.join(root, 'public/logo-gopher.svg')),
  ]);
  return { regular, semibold, logo: `data:image/svg+xml;base64,${logo.toString('base64')}` };
}

let assets: ReturnType<typeof loadAssets> | undefined;

// Step bars on the right, like a run's steps laid out over time.
const bars = [
  { left: 0, top: 70, width: 480 },
  { left: 0, top: 140, width: 100 },
  { left: 112, top: 208, width: 74 },
  { left: 112, top: 277, width: 74 },
  { left: 188, top: 346, width: 134 },
  { left: 334, top: 415, width: 146 },
  { left: 334, top: 484, width: 146 },
];
const columns = [0, 111, 222, 333];

/** Renders a page's Open Graph image: the logo, "go-workflow:" and the title, and an optional description. */
export async function renderOGImage({ title, description }: { title: string; description?: string }) {
  const { regular, semibold, logo } = await (assets ??= loadAssets());

  return new ImageResponse(
    <div
      style={{
        display: 'flex',
        width: '100%',
        height: '100%',
        background: '#000',
        color: '#ededed',
        fontFamily: 'Geist',
      }}
    >
      <div
        style={{
          display: 'flex',
          position: 'relative',
          flexDirection: 'column',
          justifyContent: 'center',
          width: 700,
          // Keep text clear of the logo above it.
          padding: '160px 0 48px 80px',
        }}
      >
        <img src={logo} width={66} height={88} style={{ position: 'absolute', top: 64, left: 80 }} />
        <div style={{ display: 'flex', flexWrap: 'wrap', fontSize: 68, fontWeight: 600, lineHeight: 1.08, letterSpacing: -2 }}>
          <span style={{ color: '#ededed', marginRight: 18 }}>{appName}:</span>
          <span style={{ color: '#8f8f8f' }}>{title}</span>
        </div>
        {description && (
          <div style={{ marginTop: 28, fontSize: 22, lineHeight: 1.45, color: '#8f8f8f', maxWidth: 600 }}>
            {description}
          </div>
        )}
      </div>
      <div style={{ display: 'flex', position: 'relative', flex: 1 }}>
        {columns.map((left) => (
          <div
            key={left}
            style={{
              position: 'absolute',
              top: 0,
              bottom: 0,
              left: 58 + left,
              borderLeft: '1px dashed #3a3a3a',
            }}
          />
        ))}
        {bars.map((bar, i) => (
          <div
            key={i}
            style={{
              position: 'absolute',
              left: 64 + bar.left,
              top: bar.top,
              width: bar.width,
              height: 56,
              borderRadius: 10,
              border: '1.5px solid #3d3d3d',
              background: '#111',
            }}
          />
        ))}
      </div>
    </div>,
    {
      ...ogSize,
      fonts: [
        { name: 'Geist', data: regular, weight: 400, style: 'normal' },
        { name: 'Geist', data: semibold, weight: 600, style: 'normal' },
      ],
    },
  );
}
