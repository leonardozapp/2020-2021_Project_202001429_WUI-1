<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\RelBenchmarksClasseFundo $relBenchmarksClasseFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('Edit Rel Benchmarks Classe Fundo'), ['action' => 'edit', $relBenchmarksClasseFundo->tipo_benchmark_id], ['class' => 'side-nav-item']) ?>
            <?= $this->Form->postLink(__('Delete Rel Benchmarks Classe Fundo'), ['action' => 'delete', $relBenchmarksClasseFundo->tipo_benchmark_id], ['confirm' => __('Are you sure you want to delete # {0}?', $relBenchmarksClasseFundo->tipo_benchmark_id), 'class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('List Rel Benchmarks Classe Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
            <?= $this->Html->link(__('New Rel Benchmarks Classe Fundo'), ['action' => 'add'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="relBenchmarksClasseFundos view content">
            <h3><?= h($relBenchmarksClasseFundo->tipo_benchmark_id) ?></h3>
            <table>
                <tr>
                    <th><?= __('Tipo Benchmark') ?></th>
                    <td><?= $relBenchmarksClasseFundo->has('tipo_benchmark') ? $this->Html->link($relBenchmarksClasseFundo->tipo_benchmark->id, ['controller' => 'TipoBenchmarks', 'action' => 'view', $relBenchmarksClasseFundo->tipo_benchmark->id]) : '' ?></td>
                </tr>
                <tr>
                    <th><?= __('Tipo Classe Fundo') ?></th>
                    <td><?= $relBenchmarksClasseFundo->has('tipo_classe_fundo') ? $this->Html->link($relBenchmarksClasseFundo->tipo_classe_fundo->classe, ['controller' => 'TipoClasseFundos', 'action' => 'view', $relBenchmarksClasseFundo->tipo_classe_fundo->id]) : '' ?></td>
                </tr>
            </table>
        </div>
    </div>
</div>
