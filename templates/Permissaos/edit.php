<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\Permissao $permissao
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $permissao->id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $permissao->id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Permissaos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="permissaos form content">
            <?= $this->Form->create($permissao) ?>
            <fieldset>
                <legend><?= __('Edit Permissao') ?></legend>
                <?php
                    echo $this->Form->control('supor_fck_root');
                    echo $this->Form->control('administrador_mng');
                    echo $this->Form->control('carteiras_mng');
                    echo $this->Form->control('logs_mng');
                    echo $this->Form->control('operacoes_mng');
                    echo $this->Form->control('usuarios_mng');
                    echo $this->Form->control('tipos_mng');
                    echo $this->Form->control('rel_mng');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
