<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\IndicadoresCarteira $indicadoresCarteira
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Indicadores Carteiras'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="indicadoresCarteiras form content">
            <?= $this->Form->create($indicadoresCarteira) ?>
            <fieldset>
                <legend><?= __('Add Indicadores Carteira') ?></legend>
                <?php
                    echo $this->Form->control('rentabilidade');
                    echo $this->Form->control('desvio_padrao');
                    echo $this->Form->control('num_valores');
                    echo $this->Form->control('rentab_min');
                    echo $this->Form->control('rentab_max');
                    echo $this->Form->control('max_drawdown');
                    echo $this->Form->control('tipo_benchmark_id', ['options' => $tipoBenchmarks, 'empty' => true]);
                    echo $this->Form->control('meses_acima_bench');
                    echo $this->Form->control('sharpe');
                    echo $this->Form->control('beta');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
