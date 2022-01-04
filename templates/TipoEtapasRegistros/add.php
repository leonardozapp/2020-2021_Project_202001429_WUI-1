<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\TipoEtapasRegistro $tipoEtapasRegistro
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Tipo Etapas Registros'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="tipoEtapasRegistros form content">
            <?= $this->Form->create($tipoEtapasRegistro) ?>
            <fieldset>
                <legend><?= __('Add Tipo Etapas Registro') ?></legend>
                <?php
                    echo $this->Form->control('fase');
                    echo $this->Form->control('etapa');
                    echo $this->Form->control('podeLogar');
                    echo $this->Form->control('podeInvestir');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
