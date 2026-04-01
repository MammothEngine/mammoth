import { useTheme, ThemeProvider } from './components/theme-provider';
import { Button } from './components/ui/button';
import {
  Database,
  Zap,
  Shield,
  Server,
  Globe,
  Lock,
  Moon,
  Sun,
  Menu,
  X,
  ArrowRight,
  Code,
  MessageCircle,
  Copy,
  Check,
  Clock,
  TrendingUp,
  Users,
  HardDrive,
} from 'lucide-react';
import { useState, useEffect } from 'react';

// Navigation Component
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
    { href: '#features', label: 'Features' },
    { href: '#performance', label: 'Performance' },
    { href: '#docs', label: 'Documentation' },
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
          <a href="#" className="flex items-center gap-2 group">
            <div className="w-8 h-8 lg:w-10 lg:h-10 rounded-xl bg-gradient-to-br from-mammoth-500 to-mammoth-700 flex items-center justify-center group-hover:shadow-lg group-hover:shadow-mammoth-500/25 transition-all">
              <Database className="w-5 h-5 lg:w-6 lg:h-6 text-white" />
            </div>
            <span className="text-lg lg:text-xl font-bold text-foreground">
              Mammoth
            </span>
          </a>

          {/* Desktop Navigation */}
          <div className="hidden md:flex items-center gap-8">
            {navLinks.map((link) => (
              <a
                key={link.href}
                href={link.href}
                className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors"
              >
                {link.label}
              </a>
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

            <Button
              className="hidden sm:inline-flex bg-mammoth-600 hover:bg-mammoth-700 text-white"
              onClick={() => window.open('https://github.com/MammothEngine/mammoth', '_blank')}
            >
              <Code className="w-4 h-4 mr-2" />
              Star on GitHub
            </Button>

            {/* Mobile Menu Button */}
            <button
              onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
              className="md:hidden p-2 rounded-lg hover:bg-accent transition-colors"
            >
              {isMobileMenuOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
            </button>
          </div>
        </div>

        {/* Mobile Menu */}
        {isMobileMenuOpen && (
          <div className="md:hidden py-4 border-t border-border">
            <div className="flex flex-col gap-4">
              {navLinks.map((link) => (
                <a
                  key={link.href}
                  href={link.href}
                  className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors py-2"
                  onClick={() => setIsMobileMenuOpen(false)}
                >
                  {link.label}
                </a>
              ))}
              <Button
                className="w-full bg-mammoth-600 hover:bg-mammoth-700 text-white mt-2"
                onClick={() => window.open('https://github.com/MammothEngine/mammoth', '_blank')}
              >
                <Code className="w-4 h-4 mr-2" />
                Star on GitHub
              </Button>
            </div>
          </div>
        )}
      </nav>
    </header>
  );
}

// Hero Section
function HeroSection() {
  const [copied, setCopied] = useState(false);

  const copyToClipboard = () => {
    navigator.clipboard.writeText('go install github.com/mammothengine/mammoth/cmd/mammoth@latest');
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <section className="relative min-h-screen flex items-center justify-center overflow-hidden pt-20">
      {/* Background Effects */}
      <div className="absolute inset-0 bg-gradient-to-b from-mammoth-500/5 via-transparent to-transparent" />
      <div className="absolute inset-0 bg-[radial-gradient(ellipse_80%_80%_at_50%_-20%,rgba(14,165,233,0.15),transparent)]" />

      {/* Grid Pattern */}
      <div
        className="absolute inset-0 opacity-[0.03] dark:opacity-[0.05]"
        style={{
          backgroundImage: `linear-gradient(to right, currentColor 1px, transparent 1px),
                           linear-gradient(to bottom, currentColor 1px, transparent 1px)`,
          backgroundSize: '4rem 4rem',
        }}
      />

      <div className="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-20 lg:py-32 text-center">
        {/* Badge */}
        <div className="inline-flex items-center gap-2 px-4 py-2 rounded-full bg-mammoth-500/10 border border-mammoth-500/20 mb-8">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-mammoth-500 opacity-75" />
            <span className="relative inline-flex rounded-full h-2 w-2 bg-mammoth-500" />
          </span>
          <span className="text-sm font-medium text-mammoth-600 dark:text-mammoth-400">
            v0.0.1 Alpha Now Available
          </span>
        </div>

        {/* Main Heading */}
        <h1 className="text-4xl sm:text-5xl lg:text-7xl font-bold tracking-tight mb-6">
          <span className="text-foreground">The </span>
          <span className="bg-gradient-to-r from-mammoth-500 via-mammoth-400 to-mammoth-600 bg-clip-text text-transparent">
            MongoDB-Compatible
          </span>
          <br className="hidden sm:block" />
          <span className="text-foreground"> Document Database</span>
        </h1>

        {/* Subtitle */}
        <p className="text-lg sm:text-xl text-muted-foreground max-w-2xl mx-auto mb-10">
          Mammoth Engine is a high-performance, embeddable document database with MongoDB wire
          protocol compatibility. Built for speed, scale, and simplicity.
        </p>

        {/* Install Command */}
        <div className="max-w-xl mx-auto mb-10">
          <div className="relative group">
            <div className="absolute -inset-0.5 bg-gradient-to-r from-mammoth-500 to-mammoth-700 rounded-xl blur opacity-30 group-hover:opacity-50 transition duration-500" />
            <div className="relative flex items-center gap-3 px-4 py-3 sm:px-6 sm:py-4 bg-card/50 backdrop-blur-xl rounded-xl border border-border">
              <span className="text-mammoth-500 font-mono">$</span>
              <code className="flex-1 text-left font-mono text-sm sm:text-base text-foreground overflow-x-auto">
                go install github.com/mammothengine/mammoth/cmd/mammoth@latest
              </code>
              <button
                onClick={copyToClipboard}
                className="p-2 rounded-lg hover:bg-accent transition-colors flex-shrink-0"
                aria-label="Copy command"
              >
                {copied ? (
                  <Check className="w-4 h-4 text-green-500" />
                ) : (
                  <Copy className="w-4 h-4 text-muted-foreground" />
                )}
              </button>
            </div>
          </div>
        </div>

        {/* CTA Buttons */}
        <div className="flex flex-col sm:flex-row items-center justify-center gap-4">
          <Button
            size="lg"
            className="w-full sm:w-auto bg-mammoth-600 hover:bg-mammoth-700 text-white px-8 h-12 text-base shadow-lg shadow-mammoth-500/25"
          >
            Get Started
            <ArrowRight className="ml-2 w-4 h-4" />
          </Button>
          <Button
            size="lg"
            variant="outline"
            className="w-full sm:w-auto px-8 h-12 text-base"
            onClick={() => window.open('https://github.com/MammothEngine/mammoth', '_blank')}
          >
            <Code className="mr-2 w-4 h-4" />
            View on GitHub
          </Button>
        </div>

        {/* Stats */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-6 sm:gap-8 mt-20">
          {[
            { value: '50K+', label: 'Writes/sec' },
            { value: '100K+', label: 'Reads/sec' },
            { value: '<1ms', label: 'P50 Latency' },
            { value: '26', label: 'Benchmarks' },
          ].map((stat) => (
            <div key={stat.label} className="text-center">
              <div className="text-2xl sm:text-3xl font-bold text-foreground">{stat.value}</div>
              <div className="text-sm text-muted-foreground">{stat.label}</div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

// Features Section
function FeaturesSection() {
  const features = [
    {
      icon: Database,
      title: 'MongoDB Compatible',
      description:
        'Drop-in replacement for MongoDB applications. Supports the wire protocol, BSON, and all major MongoDB features.',
    },
    {
      icon: Zap,
      title: 'High Performance',
      description:
        'LSM-Tree storage engine optimized for write-heavy workloads. 50K+ writes/sec and 100K+ reads/sec on a single node.',
    },
    {
      icon: Shield,
      title: 'ACID Transactions',
      description:
        'Full multi-document transaction support with snapshot isolation. Consistent and reliable data operations.',
    },
    {
      icon: Server,
      title: 'Raft Replication',
      description:
        'Built-in consensus-based replication for high availability. Automatic failover and leader election.',
    },
    {
      icon: Globe,
      title: 'Horizontal Scaling',
      description:
        'Native sharding support for distributing data across clusters. Scale out as your data grows.',
    },
    {
      icon: Lock,
      title: 'Enterprise Security',
      description:
        'AES-256-GCM encryption at rest, TLS for data in transit, and comprehensive authentication.',
    },
  ];

  return (
    <section id="features" className="py-24 lg:py-32 relative">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section Header */}
        <div className="text-center max-w-3xl mx-auto mb-16 lg:mb-20">
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-foreground mb-6">
            Everything you need to{' '}
            <span className="bg-gradient-to-r from-mammoth-500 to-mammoth-400 bg-clip-text text-transparent">
              scale your data
            </span>
          </h2>
          <p className="text-lg text-muted-foreground">
            Mammoth Engine combines the flexibility of document databases with the performance of
            modern storage engines.
          </p>
        </div>

        {/* Features Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 lg:gap-8">
          {features.map((feature) => (
            <div
              key={feature.title}
              className="group relative p-6 lg:p-8 rounded-2xl bg-card border border-border hover:border-mammoth-500/50 transition-all duration-300 hover:shadow-xl hover:shadow-mammoth-500/10"
            >
              <div className="w-12 h-12 rounded-xl bg-mammoth-500/10 flex items-center justify-center mb-4 group-hover:bg-mammoth-500/20 transition-colors">
                <feature.icon className="w-6 h-6 text-mammoth-500" />
              </div>
              <h3 className="text-xl font-semibold text-foreground mb-2">{feature.title}</h3>
              <p className="text-muted-foreground leading-relaxed">{feature.description}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}

// Performance Section
function PerformanceSection() {
  return (
    <section id="performance" className="py-24 lg:py-32 relative overflow-hidden">
      {/* Background */}
      <div className="absolute inset-0 bg-gradient-to-b from-mammoth-950/20 via-background to-background" />

      <div className="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid lg:grid-cols-2 gap-12 lg:gap-16 items-center">
          {/* Content */}
          <div>
            <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-foreground mb-6">
              Performance that{' '}
              <span className="bg-gradient-to-r from-mammoth-500 to-mammoth-400 bg-clip-text text-transparent">
                scales with you
              </span>
            </h2>
            <p className="text-lg text-muted-foreground mb-8">
              Built on an LSM-Tree architecture with advanced caching, Mammoth Engine delivers
              exceptional performance for both read and write-heavy workloads.
            </p>

            <div className="space-y-4">
              {[
                { icon: TrendingUp, label: '50K+ writes per second', sublabel: 'Single node performance' },
                { icon: Clock, label: '< 1ms P50 latency', sublabel: 'Consistent low-latency reads' },
                { icon: HardDrive, label: 'Efficient storage', sublabel: 'Compression and compaction' },
                { icon: Users, label: 'Concurrent access', sublabel: 'Lock-free read paths' },
              ].map((item) => (
                <div key={item.label} className="flex items-start gap-4">
                  <div className="w-10 h-10 rounded-lg bg-mammoth-500/10 flex items-center justify-center flex-shrink-0">
                    <item.icon className="w-5 h-5 text-mammoth-500" />
                  </div>
                  <div>
                    <div className="font-semibold text-foreground">{item.label}</div>
                    <div className="text-sm text-muted-foreground">{item.sublabel}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Code Example */}
          <div className="relative">
            <div className="absolute -inset-4 bg-gradient-to-r from-mammoth-500/20 to-mammoth-700/20 rounded-3xl blur-2xl" />
            <div className="relative rounded-2xl overflow-hidden border border-border bg-card/50 backdrop-blur-xl">
              <div className="flex items-center gap-2 px-4 py-3 border-b border-border bg-muted/50">
                <div className="w-3 h-3 rounded-full bg-red-500" />
                <div className="w-3 h-3 rounded-full bg-yellow-500" />
                <div className="w-3 h-3 rounded-full bg-green-500" />
                <span className="ml-2 text-sm text-muted-foreground">example.go</span>
              </div>
              <pre className="p-6 text-sm overflow-x-auto">
                <code className="font-mono text-foreground">
                  {`package main

import (
    "context"
    "log"

    "github.com/mammothengine/mammoth/pkg/mongo"
)

func main() {
    // Connect to Mammoth Engine
    client, err := mongo.Connect(
        context.Background(),
        "mongodb://localhost:27017",
    )
    if err != nil {
        log.Fatal(err)
    }

    // Insert a document
    db := client.Database("mydb")
    coll := db.Collection("users")

    _, err = coll.InsertOne(context.Background(), map[string]any{
        "name": "John Doe",
        "email": "john@example.com",
    })
    if err != nil {
        log.Fatal(err)
    }
}`}
                </code>
              </pre>
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

// Footer
function Footer() {
  return (
    <footer className="border-t border-border py-12 lg:py-16">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8 lg:gap-12 mb-12">
          {/* Brand */}
          <div className="col-span-2 md:col-span-1">
            <a href="#" className="flex items-center gap-2 mb-4">
              <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-mammoth-500 to-mammoth-700 flex items-center justify-center">
                <Database className="w-5 h-5 text-white" />
              </div>
              <span className="text-lg font-bold text-foreground">Mammoth</span>
            </a>
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
              <a
                href="#"
                className="p-2 rounded-lg bg-muted hover:bg-accent transition-colors"
              >
                <MessageCircle className="w-4 h-4" />
              </a>
            </div>
          </div>

          {/* Links */}
          <div>
            <h4 className="font-semibold text-foreground mb-4">Product</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a href="#features" className="text-muted-foreground hover:text-foreground transition-colors">
                  Features
                </a>
              </li>
              <li>
                <a href="#performance" className="text-muted-foreground hover:text-foreground transition-colors">
                  Performance
                </a>
              </li>
              <li>
                <a href="#" className="text-muted-foreground hover:text-foreground transition-colors">
                  Roadmap
                </a>
              </li>
            </ul>
          </div>

          <div>
            <h4 className="font-semibold text-foreground mb-4">Resources</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a href="#" className="text-muted-foreground hover:text-foreground transition-colors">
                  Documentation
                </a>
              </li>
              <li>
                <a href="#" className="text-muted-foreground hover:text-foreground transition-colors">
                  API Reference
                </a>
              </li>
              <li>
                <a href="#" className="text-muted-foreground hover:text-foreground transition-colors">
                  Examples
                </a>
              </li>
            </ul>
          </div>

          <div>
            <h4 className="font-semibold text-foreground mb-4">Community</h4>
            <ul className="space-y-2 text-sm">
              <li>
                <a
                  href="https://github.com/MammothEngine/mammoth"
                  className="text-muted-foreground hover:text-foreground transition-colors"
                >
                  GitHub
                </a>
              </li>
              <li>
                <a href="#" className="text-muted-foreground hover:text-foreground transition-colors">
                  Discord
                </a>
              </li>
            </ul>
          </div>
        </div>

        {/* Bottom */}
        <div className="pt-8 border-t border-border flex flex-col sm:flex-row items-center justify-between gap-4">
          <p className="text-sm text-muted-foreground">
            © {new Date().getFullYear()} Mammoth Engine. MIT License.
          </p>
          <p className="text-sm text-muted-foreground">
            Built with ❤️ by the Mammoth Engine team.
          </p>
        </div>
      </div>
    </footer>
  );
}

// Main App
function App() {
  return (
    <ThemeProvider defaultTheme="system" storageKey="mammoth-theme">
      <div className="min-h-screen bg-background text-foreground">
        <Navbar />
        <main>
          <HeroSection />
          <FeaturesSection />
          <PerformanceSection />
        </main>
        <Footer />
      </div>
    </ThemeProvider>
  );
}

export default App;
