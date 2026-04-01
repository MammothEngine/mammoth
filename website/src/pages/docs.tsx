import { useState, useEffect } from 'react';
import { useParams, useNavigate, Link } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { docsNavigation, getDocContent } from '../data/docs';
import { useTheme } from '../components/theme-provider';
import {
  Database,
  Moon,
  Sun,
  Menu,
  X,
  Code,
  ChevronRight,
  BookOpen,
} from 'lucide-react';

// Main Navbar (same as landing page)
function Navbar() {
  const { resolvedTheme, setTheme } = useTheme();
  const [isScrolled, setIsScrolled] = useState(false);
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);

  useEffect(() => {
    const handleScroll = () => {
      setIsScrolled(window.scrollY > 50);
    };
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  const navLinks = [
    { to: '/', label: 'Home' },
    { to: '/#features', label: 'Features' },
    { to: '/#performance', label: 'Performance' },
    { to: '/docs', label: 'Documentation' },
  ];

  return (
    <header
      className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 ${
        isScrolled
          ? 'bg-background/80 backdrop-blur-xl border-b border-border/50'
          : 'bg-transparent'
      }`}
    >
      <nav className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16 lg:h-20">
          {/* Logo */}
          <Link to="/" className="flex items-center gap-2 group">
            <div className="w-8 h-8 lg:w-10 lg:h-10 rounded-xl bg-gradient-to-br from-mammoth-500 to-mammoth-700 flex items-center justify-center group-hover:shadow-lg group-hover:shadow-mammoth-500/25 transition-all">
              <Database className="w-5 h-5 lg:w-6 lg:h-6 text-white" />
            </div>
            <span className="text-lg lg:text-xl font-bold text-foreground">
              Mammoth
            </span>
          </Link>

          {/* Desktop Navigation */}
          <div className="hidden md:flex items-center gap-8">
            {navLinks.map((link) => (
              <Link
                key={link.label}
                to={link.to}
                className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors"
              >
                {link.label}
              </Link>
            ))}
          </div>

          {/* Actions */}
          <div className="flex items-center gap-4">
            <button
              onClick={() => setTheme(resolvedTheme === 'dark' ? 'light' : 'dark')}
              className="p-2 rounded-lg hover:bg-accent transition-colors"
              aria-label="Toggle theme"
            >
              {resolvedTheme === 'dark' ? (
                <Sun className="w-5 h-5" />
              ) : (
                <Moon className="w-5 h-5" />
              )}
            </button>

            <a
              href="https://github.com/MammothEngine/mammoth"
              target="_blank"
              rel="noopener noreferrer"
              className="hidden sm:flex items-center gap-2 px-4 py-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors"
            >
              <Code className="w-4 h-4" />
              GitHub
            </a>

            {/* Mobile menu button */}
            <button
              onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
              className="md:hidden p-2 rounded-lg hover:bg-accent transition-colors"
              aria-label="Toggle menu"
            >
              {isMobileMenuOpen ? (
                <X className="w-6 h-6" />
              ) : (
                <Menu className="w-6 h-6" />
              )}
            </button>
          </div>
        </div>

        {/* Mobile Navigation */}
        {isMobileMenuOpen && (
          <div className="md:hidden py-4 border-t border-border">
            <div className="flex flex-col gap-2">
              {navLinks.map((link) => (
                <Link
                  key={link.label}
                  to={link.to}
                  onClick={() => setIsMobileMenuOpen(false)}
                  className="px-4 py-2 text-sm font-medium text-muted-foreground hover:text-foreground hover:bg-accent rounded-lg transition-colors"
                >
                  {link.label}
                </Link>
              ))}
            </div>
          </div>
        )}
      </nav>
    </header>
  );
}

// Docs-specific Navbar (appears below main navbar)
function DocsNavbar({ onMenuToggle }: { onMenuToggle: () => void }) {
  return (
    <div className="sticky top-16 lg:top-20 z-40 bg-background/95 backdrop-blur-xl border-b border-border">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center gap-4 h-12">
          <button
            onClick={onMenuToggle}
            className="lg:hidden p-2 rounded-lg hover:bg-accent transition-colors"
          >
            <Menu className="w-5 h-5" />
          </button>
          <Link to="/" className="flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors">
            <span className="text-sm">Home</span>
          </Link>
          <ChevronRight className="w-4 h-4 text-muted-foreground" />
          <Link to="/docs" className="flex items-center gap-2 text-muted-foreground hover:text-foreground transition-colors">
            <BookOpen className="w-4 h-4" />
            <span className="text-sm font-medium">Documentation</span>
          </Link>
        </div>
      </div>
    </div>
  );
}

function Sidebar({
  activeSlug,
  isOpen,
  onClose,
}: {
  activeSlug: string;
  isOpen: boolean;
  onClose: () => void;
}) {
  const navigate = useNavigate();

  return (
    <>
      {/* Mobile overlay */}
      {isOpen && (
        <div
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={onClose}
        />
      )}

      <aside
        className={`fixed lg:sticky top-[7.5rem] lg:top-32 left-0 z-50 w-72 h-[calc(100vh-7.5rem)] lg:h-[calc(100vh-8rem)] bg-background border-r border-border overflow-y-auto transition-transform duration-300 lg:translate-x-0 ${
          isOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="p-4">
          <div className="flex items-center justify-between mb-4">
            <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider">
              Documentation
            </h2>
            <button
              onClick={onClose}
              className="lg:hidden p-1 rounded hover:bg-accent"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          <nav className="space-y-6">
            {docsNavigation.map((section) => (
              <div key={section.title}>
                <h3 className="text-sm font-medium text-foreground mb-2">
                  {section.title}
                </h3>
                <ul className="space-y-1">
                  {section.items.map((item) => (
                    <li key={item.slug}>
                      <button
                        onClick={() => {
                          navigate(`/docs/${item.slug}`);
                          onClose();
                        }}
                        className={`w-full text-left text-sm px-3 py-2 rounded-lg transition-colors ${
                          activeSlug === item.slug
                            ? 'bg-mammoth-500/10 text-mammoth-600 font-medium'
                            : 'text-muted-foreground hover:text-foreground hover:bg-accent'
                        }`}
                      >
                        {item.title}
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </nav>
        </div>
      </aside>
    </>
  );
}

function DocsContent({ slug }: { slug: string }) {
  const content = getDocContent(slug);
  const docTitle = docsNavigation
    .flatMap((s) => s.items)
    .find((i) => i.slug === slug)?.title || 'Documentation';

  return (
    <article className="prose prose-slate dark:prose-invert max-w-none">
      <div className="mb-8 pb-8 border-b border-border">
        <h1 className="text-3xl sm:text-4xl font-bold text-foreground mb-4">
          {docTitle}
        </h1>
        <p className="text-lg text-muted-foreground">
          Mammoth Engine documentation - {docTitle.toLowerCase()} guide
        </p>
      </div>

      <div className="prose-headings:text-foreground prose-h2:text-2xl prose-h2:font-semibold prose-h2:mt-8 prose-h2:mb-4 prose-h3:text-xl prose-h3:font-medium prose-h3:mt-6 prose-h3:mb-3 prose-p:text-muted-foreground prose-p:leading-relaxed prose-a:text-mammoth-500 prose-a:no-underline hover:prose-a:underline prose-code:text-mammoth-500 prose-code:bg-mammoth-500/10 prose-code:px-1.5 prose-code:py-0.5 prose-code:rounded prose-code:before:content-none prose-code:after:content-none prose-pre:bg-card prose-pre:border prose-pre:border-border prose-pre:rounded-xl prose-pre:p-4 prose-pre:overflow-x-auto prose-table:w-full prose-table:border-collapse prose-th:text-left prose-th:text-sm prose-th:font-semibold prose-th:text-foreground prose-th:p-3 prose-th:border-b prose-td:text-sm prose-td:text-muted-foreground prose-td:p-3 prose-td:border-b prose-td:border-border prose-li:text-muted-foreground prose-li:my-1">
        <ReactMarkdown remarkPlugins={[remarkGfm]}>
          {content}
        </ReactMarkdown>
      </div>

      <div className="mt-12 pt-8 border-t border-border">
        <p className="text-sm text-muted-foreground">
          Was this page helpful?{' '}
          <a
            href="https://github.com/MammothEngine/mammoth/issues"
            target="_blank"
            rel="noopener noreferrer"
            className="text-mammoth-500 hover:underline"
          >
            Let us know
          </a>
        </p>
      </div>
    </article>
  );
}

// Footer (same as landing page)
function Footer() {
  return (
    <footer className="border-t border-border py-12 lg:py-16 mt-auto">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8 lg:gap-12 mb-12">
          {/* Brand */}
          <div className="col-span-2 md:col-span-1">
            <Link to="/" className="flex items-center gap-2 mb-4">
              <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-mammoth-500 to-mammoth-700 flex items-center justify-center">
                <Database className="w-5 h-5 text-white" />
              </div>
              <span className="text-lg font-bold text-foreground">Mammoth</span>
            </Link>
            <p className="text-sm text-muted-foreground mb-4">
              High-performance MongoDB-compatible document database engine built in Go.
            </p>
            <div className="flex items-center gap-3">
              <a
                href="https://github.com/MammothEngine/mammoth"
                target="_blank"
                rel="noopener noreferrer"
                className="p-2 rounded-lg bg-muted hover:bg-accent transition-colors"
              >
                <Code className="w-4 h-4" />
              </a>
            </div>
          </div>

          {/* Links */}
          <div>
            <h4 className="font-semibold text-foreground mb-4">Product</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <Link to="/#features" className="text-muted-foreground hover:text-foreground transition-colors">
                  Features
                </Link>
              </li>
              <li>
                <Link to="/#performance" className="text-muted-foreground hover:text-foreground transition-colors">
                  Performance
                </Link>
              </li>
              <li>
                <Link to="/docs" className="text-muted-foreground hover:text-foreground transition-colors">
                  Documentation
                </Link>
              </li>
            </ul>
          </div>

          <div>
            <h4 className="font-semibold text-foreground mb-4">Resources</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a
                  href="https://github.com/MammothEngine/mammoth"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-muted-foreground hover:text-foreground transition-colors"
                >
                  GitHub
                </a>
              </li>
              <li>
                <Link to="/docs/quickstart" className="text-muted-foreground hover:text-foreground transition-colors">
                  Quick Start
                </Link>
              </li>
              <li>
                <Link to="/docs/installation" className="text-muted-foreground hover:text-foreground transition-colors">
                  Installation
                </Link>
              </li>
            </ul>
          </div>

          <div>
            <h4 className="font-semibold text-foreground mb-4">Docs</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <Link to="/docs/architecture" className="text-muted-foreground hover:text-foreground transition-colors">
                  Architecture
                </Link>
              </li>
              <li>
                <Link to="/docs/api-overview" className="text-muted-foreground hover:text-foreground transition-colors">
                  API Reference
                </Link>
              </li>
              <li>
                <Link to="/docs/deployment" className="text-muted-foreground hover:text-foreground transition-colors">
                  Deployment
                </Link>
              </li>
            </ul>
          </div>
        </div>

        {/* Bottom */}
        <div className="pt-8 border-t border-border flex flex-col sm:flex-row items-center justify-between gap-4">
          <p className="text-sm text-muted-foreground">
            © 2026 Mammoth Engine. Open source under MIT License.
          </p>
          <div className="flex items-center gap-6">
            <a
              href="https://github.com/MammothEngine/mammoth/blob/main/LICENSE"
              target="_blank"
              rel="noopener noreferrer"
              className="text-sm text-muted-foreground hover:text-foreground transition-colors"
            >
              License
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}

export default function DocsPage() {
  const { slug = 'intro' } = useParams<{ slug: string }>();
  const [sidebarOpen, setSidebarOpen] = useState(false);

  // Close sidebar on route change (mobile)
  useEffect(() => {
    setSidebarOpen(false);
  }, [slug]);

  return (
    <div className="min-h-screen bg-background text-foreground flex flex-col">
      <Navbar />
      <DocsNavbar onMenuToggle={() => setSidebarOpen(!sidebarOpen)} />

      <div className="max-w-7xl mx-auto flex flex-1 w-full">
        <Sidebar
          activeSlug={slug}
          isOpen={sidebarOpen}
          onClose={() => setSidebarOpen(false)}
        />

        <main className="flex-1 min-w-0 lg:ml-0">
          <div className="px-4 sm:px-6 lg:px-8 py-8 pt-8">
            <DocsContent slug={slug} />
          </div>
        </main>

        {/* Table of Contents (placeholder for future) */}
        <div className="hidden xl:block w-64 sticky top-32 h-[calc(100vh-8rem)] overflow-y-auto px-4">
          <h4 className="text-sm font-semibold text-muted-foreground uppercase tracking-wider mb-4">
            On this page
          </h4>
          <p className="text-sm text-muted-foreground">
            Quick navigation coming soon...
          </p>
        </div>
      </div>

      <Footer />
    </div>
  );
}
